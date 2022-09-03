/* SPDX-License-Identifier: GPL-2.0-or-later
 * Copyright 2016 Kévin Dietrich. All rights reserved. */

/** \file
 * \ingroup balembic
 */

#include "abc_reader_curves.h"
#include "abc_axis_conversion.h"
#include "abc_customdata.h"
#include "abc_reader_transform.h"
#include "abc_util.h"

#include <cstdio>

#include "MEM_guardedalloc.h"

#include "DNA_curves_types.h"
#include "DNA_object_types.h"

#include "BLI_listbase.h"

#include "BKE_curves.h"
#include "BKE_curves.hh"
#include "BKE_geometry_set.hh"
#include "BKE_mesh.h"
#include "BKE_object.h"
#include "BKE_spline.hh"

using Alembic::Abc::FloatArraySamplePtr;
using Alembic::Abc::Int32ArraySamplePtr;
using Alembic::Abc::P3fArraySamplePtr;
using Alembic::Abc::PropertyHeader;
using Alembic::Abc::UcharArraySamplePtr;

using Alembic::AbcGeom::CurvePeriodicity;
using Alembic::AbcGeom::ICompoundProperty;
using Alembic::AbcGeom::ICurves;
using Alembic::AbcGeom::ICurvesSchema;
using Alembic::AbcGeom::IFloatGeomParam;
using Alembic::AbcGeom::IInt16Property;
using Alembic::AbcGeom::ISampleSelector;
using Alembic::AbcGeom::kWrapExisting;

namespace blender::io::alembic {

static int16_t get_curve_resolution(const ICurvesSchema &schema,
                                    const Alembic::Abc::ISampleSelector &sample_sel)
{
  ICompoundProperty user_props = schema.getUserProperties();
  if (user_props) {
    const PropertyHeader *header = user_props.getPropertyHeader(ABC_CURVE_RESOLUTION_U_PROPNAME);
    if (header != nullptr && header->isScalar() && IInt16Property::matches(*header)) {
      IInt16Property resolu(user_props, header->getName());
      return resolu.getValue(sample_sel);
    }
  }

  return 1;
}

static int16_t get_curve_order(Alembic::AbcGeom::CurveType abc_curve_type,
                               const UcharArraySamplePtr orders,
                               size_t curve_index)
{
  switch (abc_curve_type) {
    case Alembic::AbcGeom::kCubic:
      return 4;
    case Alembic::AbcGeom::kVariableOrder:
      if (orders && orders->size() > curve_index) {
        return static_cast<int16_t>((*orders)[curve_index]);
      }
      ATTR_FALLTHROUGH;
    case Alembic::AbcGeom::kLinear:
    default:
      return 2;
  }
}

static int get_curve_overlap(Alembic::AbcGeom::CurvePeriodicity periodicity,
                             const P3fArraySamplePtr positions,
                             int idx,
                             int num_verts,
                             int16_t order)
{
  if (periodicity != Alembic::AbcGeom::kPeriodic) {
    /* kNonPeriodic is always assumed to have no overlap. */
    return 0;
  }

  /* Check the number of points which overlap, we don't have overlapping points in Blender, but
   * other software do use them to indicate that a curve is actually cyclic. Usually the number of
   * overlapping points is equal to the order/degree of the curve.
   */

  const int start = idx;
  const int end = idx + num_verts;
  int overlap = 0;

  for (int j = start, k = end - order; j < order; j++, k++) {
    const Imath::V3f &p1 = (*positions)[j];
    const Imath::V3f &p2 = (*positions)[k];

    if (p1 != p2) {
      break;
    }

    overlap++;
  }

  /* TODO: Special case, need to figure out how it coincides with knots. */
  if (overlap == 0 && num_verts > 2 && (*positions)[start] == (*positions)[end - 1]) {
    overlap = 1;
  }

  /* There is no real cycles. */
  return overlap;
}

static CurveType get_curve_type(Alembic::AbcGeom::BasisType basis)
{
  switch (basis) {
    case Alembic::AbcGeom::kNoBasis:
      return CURVE_TYPE_POLY;
    case Alembic::AbcGeom::kBezierBasis:
      return CURVE_TYPE_BEZIER;
    case Alembic::AbcGeom::kBsplineBasis:
      return CURVE_TYPE_NURBS;
    case Alembic::AbcGeom::kCatmullromBasis:
      return CURVE_TYPE_CATMULL_ROM;
    case Alembic::AbcGeom::kHermiteBasis:
    case Alembic::AbcGeom::kPowerBasis:
      /* Those types are unknown to Blender, use a default poly type. */
      return CURVE_TYPE_POLY;
  }
  return CURVE_TYPE_POLY;
}

static bool curves_topology_changed(const bke::CurvesGeometry &geometry,
                                    Span<int> preprocessed_offsets)
{
  /* Offsets have an extra element. */
  if (geometry.curve_num != preprocessed_offsets.size() - 1) {
    return true;
  }

  const Span<int> offsets = geometry.offsets();
  for (const int i_curve : preprocessed_offsets.index_range()) {
    if (offsets[i_curve] != preprocessed_offsets[i_curve]) {
      return true;
    }
  }

  return false;
}

struct PreprocessedSampleData {
  Vector<int> offset_in_blender;
  Vector<int> offset_in_alembic;
  Vector<bool> curves_overlaps;
  Vector<int8_t> curves_orders;
  bool do_cyclic = false;

  CurveType curve_type;

  /* Store the pointers during preprocess so we do not have to look up the sample twice. */
  P3fArraySamplePtr positions = nullptr;
  FloatArraySamplePtr weights = nullptr;
  FloatArraySamplePtr radii = nullptr;
};

/* Compute topological information about the curves. We do this step mainly to properly account
 * for curves overlaps which imply different offsets between Blender and Alembic, but also to
 * validate the data and cache some values. */
static std::optional<PreprocessedSampleData> preprocess_sample(StringRefNull iobject_name,
                                                               const ICurvesSchema &schema,
                                                               const ISampleSelector sample_sel)
{

  ICurvesSchema::Sample smp;
  try {
    smp = schema.getValue(sample_sel);
  }
  catch (Alembic::Util::Exception &ex) {
    printf("Alembic: error reading curve sample for '%s/%s' at time %f: %s\n",
           iobject_name.c_str(),
           schema.getName().c_str(),
           sample_sel.getRequestedTime(),
           ex.what());
    return {};
  }

  /* Note: although Alembic can store knots, we do not read them as the functionality is not
   * exposed by the Curves API yet. */
  const Int32ArraySamplePtr per_curve_vertices_count = smp.getCurvesNumVertices();
  const P3fArraySamplePtr positions = smp.getPositions();
  const FloatArraySamplePtr weights = smp.getPositionWeights();
  const CurvePeriodicity periodicity = smp.getWrap();
  const UcharArraySamplePtr orders = smp.getOrders();

  const IFloatGeomParam widths_param = schema.getWidthsParam();
  FloatArraySamplePtr radii;
  if (widths_param.valid()) {
    IFloatGeomParam::Sample wsample = widths_param.getExpandedValue(sample_sel);
    radii = wsample.getVals();
  }

  const int curve_count = per_curve_vertices_count->size();

  PreprocessedSampleData data;
  /* Add 1 as these store offsets with the actual value being `offset[i + 1] - offset[i]`. */
  data.offset_in_blender.resize(curve_count + 1);
  data.offset_in_alembic.resize(curve_count + 1);
  data.curves_overlaps.resize(curve_count);
  data.curve_type = get_curve_type(smp.getBasis());

  if (data.curve_type == CURVE_TYPE_NURBS) {
    data.curves_orders.resize(curve_count);
  }

  /* Compute topological information. */

  int blender_offset = 0;
  int alembic_offset = 0;
  for (size_t i = 0; i < per_curve_vertices_count->size(); i++) {
    const int vertices_count = (*per_curve_vertices_count)[i];

    const int curve_order = get_curve_order(smp.getType(), orders, i);

    /* Check if the curve is cyclic. */
    const int overlap = get_curve_overlap(
        periodicity, positions, alembic_offset, vertices_count, curve_order);

    data.offset_in_blender[i] = blender_offset;
    data.offset_in_alembic[i] = alembic_offset;
    data.curves_overlaps[i] = overlap != 0;

    if (data.curve_type == CURVE_TYPE_NURBS) {
      data.curves_orders[i] = curve_order;
    }

    data.do_cyclic |= overlap != 0;
    blender_offset += vertices_count - overlap;
    alembic_offset += vertices_count;
  }
  data.offset_in_blender[curve_count] = blender_offset;
  data.offset_in_alembic[curve_count] = alembic_offset;

  /* Store relevant pointers. */

  data.positions = positions;

  if ((weights != nullptr) && (weights->size() > 1)) {
    data.weights = weights;
  }

  if ((radii != nullptr) && (radii->size() > 1)) {
    data.radii = radii;
  }

  return data;
}

AbcCurveReader::AbcCurveReader(const Alembic::Abc::IObject &object, ImportSettings &settings)
    : AbcObjectReader(object, settings)
{
  ICurves abc_curves(object, kWrapExisting);
  m_curves_schema = abc_curves.getSchema();

  get_min_max_time(m_iobject, m_curves_schema, m_min_time, m_max_time);
}

bool AbcCurveReader::valid() const
{
  return m_curves_schema.valid();
}

bool AbcCurveReader::accepts_object_type(
    const Alembic::AbcCoreAbstract::ObjectHeader &alembic_header,
    const Object *const ob,
    const char **err_str) const
{
  if (!Alembic::AbcGeom::ICurves::matches(alembic_header)) {
    *err_str =
        "Object type mismatch, Alembic object path pointed to Curves when importing, but not "
        "any "
        "more.";
    return false;
  }

  if (ob->type != OB_CURVES) {
    *err_str = "Object type mismatch, Alembic object path points to Curves.";
    return false;
  }

  return true;
}

void AbcCurveReader::readObjectData(Main *bmain, const Alembic::Abc::ISampleSelector &sample_sel)
{
  Curves *curves = static_cast<Curves *>(BKE_curves_add(bmain, m_data_name.c_str()));

  m_object = BKE_object_add_only_object(bmain, OB_CURVES, m_object_name.c_str());
  m_object->data = curves;

  AttributeReadingHelper attribute_helper = AttributeReadingHelper::create_default();
  read_curves_sample(curves, m_curves_schema, sample_sel, attribute_helper, 1.0f, nullptr);

  if (m_settings->always_add_cache_reader || has_animations(m_curves_schema, m_settings)) {
    addCacheModifier();
  }
}

void AbcCurveReader::read_curves_sample(Curves *curves,
                                        const ICurvesSchema &schema,
                                        const ISampleSelector &sample_sel,
                                        const AttributeReadingHelper &attribute_helper,
                                        const float velocity_scale,
                                        const char **err_str)
{
  std::optional<PreprocessedSampleData> opt_preprocess = preprocess_sample(
      m_iobject.getFullName(), schema, sample_sel);

  if (!opt_preprocess) {
    return;
  }

  const PreprocessedSampleData &data = opt_preprocess.value();

  const int point_count = data.offset_in_blender.last();
  const int curve_count = data.offset_in_blender.size() - 1;

  bke::CurvesGeometry &geometry = bke::CurvesGeometry::wrap(curves->geometry);

  if (curves_topology_changed(geometry, data.offset_in_blender)) {
    geometry.resize(point_count, curve_count);
    geometry.offsets_for_write().copy_from(data.offset_in_blender);
  }

  geometry.curve_types_for_write().fill(data.curve_type);

  if (data.curve_type != CURVE_TYPE_POLY) {
    geometry.resolution_for_write().fill(get_curve_resolution(schema, sample_sel));
  }

  MutableSpan<float3> curves_positions = geometry.positions_for_write();
  for (const int i_curve : geometry.curves_range()) {
    int position_offset = data.offset_in_alembic[i_curve];
    for (const int i_point : geometry.points_for_curve(i_curve)) {
      const Imath::V3f &pos = (*data.positions)[position_offset++];
      copy_zup_from_yup(curves_positions[i_point], pos.getValue());
    }
  }

  if (data.do_cyclic) {
    geometry.cyclic_for_write().copy_from(data.curves_overlaps);
  }

  if (data.radii) {
    if (!geometry.radius) {
      geometry.radius = static_cast<float *>(CustomData_add_layer_named(
          &geometry.point_data, CD_PROP_FLOAT, CD_DEFAULT, nullptr, geometry.point_num, "radius"));
    }

    for (const int i_curve : geometry.curves_range()) {
      int position_offset = data.offset_in_alembic[i_curve];
      for (const int i_point : geometry.points_for_curve(i_curve)) {
        geometry.radius[i_point] = (*data.radii)[position_offset++];
      }
    }
  }

  if (data.curve_type == CURVE_TYPE_NURBS) {
    geometry.nurbs_orders_for_write().copy_from(data.curves_orders);

    if (data.weights) {
      MutableSpan<float> curves_weights = geometry.nurbs_weights_for_write();
      Span<float> data_weights_span = {data.weights->get(),
                                       static_cast<int64_t>(data.weights->size())};
      for (const int i_curve : geometry.curves_range()) {
        const int alembic_offset = data.offset_in_alembic[i_curve];
        const IndexRange points = geometry.points_for_curve(i_curve);
        curves_weights.slice(points).copy_from(
            data_weights_span.slice(alembic_offset, points.size()));
      }
    }
  }

  /* Attributes. */
  CDStreamConfig config;
  config.attribute_helper = &attribute_helper;
  config.time = sample_sel.getRequestedTime();
  config.modifier_error_message = err_str;

  config.geometry_component = std::make_unique<CurveComponent>();
  CurveComponent *component = static_cast<CurveComponent *>(config.geometry_component.get());
  component->replace(curves, GeometryOwnershipType::Editable);

  read_arbitrary_attributes(config, schema, {}, sample_sel, velocity_scale);
}

void AbcCurveReader::read_geometry(GeometrySet &geometry_set,
                                   const Alembic::Abc::ISampleSelector &sample_sel,
                                   const AttributeReadingHelper &attribute_helper,
                                   int /*read_flag*/,
                                   const float velocity_scale,
                                   const char **err_str)
{
  Curves *curves = geometry_set.get_curves_for_write();

  read_curves_sample(
      curves, m_curves_schema, sample_sel, attribute_helper, velocity_scale, err_str);
}

}  // namespace blender::io::alembic
