/* SPDX-License-Identifier: GPL-2.0-or-later
 * Copyright 2016 Kévin Dietrich. All rights reserved. */

/** \file
 * \ingroup balembic
 */

#include "abc_reader_points.h"
#include "abc_axis_conversion.h"
#include "abc_reader_mesh.h"
#include "abc_reader_transform.h"
#include "abc_util.h"

#include "DNA_mesh_types.h"
#include "DNA_modifier_types.h"
#include "DNA_object_types.h"
#include "DNA_pointcloud_types.h"

#include "BKE_customdata.h"
#include "BKE_geometry_set.hh"
#include "BKE_lib_id.h"
#include "BKE_mesh.h"
#include "BKE_object.h"
#include "BKE_pointcloud.h"

using namespace Alembic::AbcGeom;

namespace blender::io::alembic {

AbcPointsReader::AbcPointsReader(const Alembic::Abc::IObject &object, ImportSettings &settings)
    : AbcObjectReader(object, settings)
{
  IPoints ipoints(m_iobject, kWrapExisting);
  m_schema = ipoints.getSchema();
  get_min_max_time(m_iobject, m_schema, m_min_time, m_max_time);
}

bool AbcPointsReader::valid() const
{
  return m_schema.valid();
}

bool AbcPointsReader::accepts_object_type(
    const Alembic::AbcCoreAbstract::ObjectHeader &alembic_header,
    const Object *const ob,
    const char **err_str) const
{
  if (!Alembic::AbcGeom::IPoints::matches(alembic_header)) {
    *err_str =
        "Object type mismatch, Alembic object path pointed to Points when importing, but not any "
        "more.";
    return false;
  }

  if (ob->type != OB_POINTCLOUD) {
    *err_str = "Object type mismatch, Alembic object path points to Points.";
    return false;
  }

  return true;
}

void AbcPointsReader::readObjectData(Main *bmain, const Alembic::Abc::ISampleSelector &sample_sel)
{
  PointCloud *point_cloud = static_cast<PointCloud *>(
      BKE_pointcloud_add_default(bmain, m_data_name.c_str()));

  AttributeReadingHelper attribute_helper = AttributeReadingHelper::create_default();
  GeometrySet geometry_set = GeometrySet::create_with_pointcloud(point_cloud,
                                                                 GeometryOwnershipType::Editable);
  read_geometry(geometry_set, sample_sel, attribute_helper, 0, 1.0f, nullptr);

  PointCloud *read_point_cloud =
      geometry_set.get_component_for_write<PointCloudComponent>().release();

  if (read_point_cloud != point_cloud) {
    BKE_pointcloud_nomain_to_pointcloud(read_point_cloud, point_cloud, true);
  }

  m_object = BKE_object_add_only_object(bmain, OB_POINTCLOUD, m_object_name.c_str());
  m_object->data = point_cloud;

  if (m_settings->always_add_cache_reader || has_animations(m_schema, m_settings)) {
    addCacheModifier();
  }
}

static void read_points_interp(const P3fArraySamplePtr positions,
                               const P3fArraySamplePtr ceil_positions,
                               const float weight,
                               MutableSpan<float3> r_points)
{
  float3 tmp;
  for (size_t i = 0; i < positions->size(); i++) {
    const Imath::V3f &floor_pos = (*positions)[i];
    const Imath::V3f &ceil_pos = (*ceil_positions)[i];
    interp_v3_v3v3(tmp, floor_pos.getValue(), ceil_pos.getValue(), weight);
    copy_zup_from_yup(r_points[i], (*positions)[i].getValue());
  }
}

static void read_points(const P3fArraySamplePtr positions, MutableSpan<float3> r_points)
{
  int64_t total_points = positions->size();

  std::cerr << "joshw: positions->size: " << positions->size() << " r_points.size: " << r_points.size() << "\n";
  if (r_points.size() < total_points) {
    total_points = r_points.size();
    std::cerr << "Alembic warning: writable positions (r_points) is smaller than source point positions.\n";

  }
  for (size_t i = 0; i < total_points; i++) {
    copy_zup_from_yup(r_points[i], (*positions)[i].getValue());
  }
}

static void read_points_sample(const IPointsSchema &schema,
                               const ISampleSelector &selector,
                               CDStreamConfig &config,
                               MutableSpan<float3> r_points)
{
  Alembic::AbcGeom::IPointsSchema::Sample sample = schema.getValue(selector);

  const P3fArraySamplePtr &positions = sample.getPositions();

  ICompoundProperty prop = schema.getArbGeomParams();

  Alembic::AbcGeom::index_t i0, i1;
  const float weight = get_weight_and_index(
      config.time, schema.getTimeSampling(), schema.getNumSamples(), i0, i1);

  if (config.use_vertex_interpolation && weight != 0.0f) {
    Alembic::AbcGeom::IPointsSchema::Sample ceil_sample;
    schema.get(ceil_sample, Alembic::Abc::ISampleSelector(i1));
    P3fArraySamplePtr ceil_positions = ceil_sample.getPositions();

    read_points_interp(positions, ceil_positions, weight, r_points);
    return;
  }

  read_points(positions, r_points);
}

static void *add_customdata_pd(PointCloud *point_cloud, const char *name, int data_type)
{
  eCustomDataType cd_data_type = static_cast<eCustomDataType>(data_type);
  void *cd_ptr;
  CustomData *pdata;
  int totpoint;

  /* unsupported custom data type -- don't do anything. */
  if (!ELEM(cd_data_type, CD_MVERT)) {
    return NULL;
  }

  pdata = &point_cloud->pdata;
  cd_ptr = CustomData_get_layer_named(pdata, cd_data_type, name);
  if (cd_ptr != NULL) {
    /* layer already exists, so just return it. */
    return cd_ptr;
  }

  /* Create a new layer. */
  totpoint = point_cloud->totpoint;
  cd_ptr = CustomData_add_layer_named(pdata, cd_data_type, CD_SET_DEFAULT, NULL, totpoint, name);
  return cd_ptr;
}

void AbcPointsReader::read_geometry(GeometrySet &geometry_set,
                                    const Alembic::Abc::ISampleSelector &sample_sel,
                                    const AttributeReadingHelper &attribute_helper,
                                    int read_flag,
                                    const float velocity_scale,
                                    const char **err_str)
{
  BLI_assert(geometry_set.has_pointcloud());


  IPointsSchema::Sample sample;
  try {
    sample = m_schema.getValue(sample_sel);

    printf("Alembic: reading points sample for '%s/%s' at time %f.\n",
           m_iobject.getFullName().c_str(),
           m_schema.getName().c_str(),
           sample_sel.getRequestedTime());
  }
  catch (Alembic::Util::Exception &ex) {
    *err_str = "Error reading points sample; more detail on the console";
    printf("Alembic: error reading points sample for '%s/%s' at time %f: %s\n",
           m_iobject.getFullName().c_str(),
           m_schema.getName().c_str(),
           sample_sel.getRequestedTime(),
           ex.what());
    return;
  }


  PointCloud *existing_point_cloud = geometry_set.get_pointcloud_for_write();
  PointCloud *point_cloud = existing_point_cloud;

  const P3fArraySamplePtr &positions = sample.getPositions();

  const IFloatGeomParam widths_param = m_schema.getWidthsParam();
  FloatArraySamplePtr radii;

  if (widths_param.valid()) {
    IFloatGeomParam::Sample wsample = widths_param.getExpandedValue(sample_sel);
    radii = wsample.getVals();
  }

  /* joshw: removed this. for some reason the existing point_cloud always has 2 points (?)
     - and in the case when we have 2 points on the new data, the positions span was
       appearing with a size of 0.
  std::cerr << "joshw: previous totpoint: " << point_cloud->totpoint << "\n";
  if (point_cloud->totpoint != positions->size()) {
    std::cerr << "joshw: new pointcloud of size " << positions->size() << "\n";
    point_cloud = BKE_pointcloud_new_nomain(positions->size());
  }
  */
  point_cloud = BKE_pointcloud_new_nomain(positions->size());

  if (point_cloud->totpoint == 0) {
    std::cerr << "big problem, 0 points.\n";
    *err_str = "Error reading points sample; more detail on the console";
    return;
  }

  CDStreamConfig config;
  config.attribute_helper = &attribute_helper;
  config.time = sample_sel.getRequestedTime();
  config.use_vertex_interpolation = (read_flag & MOD_MESHSEQ_INTERPOLATE_VERTICES) != 0;
  config.modifier_error_message = err_str;

  config.geometry_component = std::make_unique<PointCloudComponent>();
  PointCloudComponent *component = static_cast<PointCloudComponent *>(
      config.geometry_component.get());
  component->replace(point_cloud, GeometryOwnershipType::Editable);

  std::optional<bke::MutableAttributeAccessor> point_attributes =
      component->attributes_for_write();

  /*void *pointsdata = add_customdata_pd(point_cloud, "position", CD_MVERT);
  float3 *layer_data = static_cast<float3 *>(pointsdata);
  MutableSpan<float3> point_position_span = MutableSpan(layer_data, point_cloud->totpoint);*/
  std::cerr << "joshw: huh?\n";

  std::cerr << "joshw: totpoint: " << point_cloud->totpoint << "\n";
  std::cerr << "joshw: positions->size: " << positions->size() << "\n";

  // joshw, bug, this is a size 0 domain for some reason?
  bke::SpanAttributeWriter<float3> point_position =
      point_attributes->lookup_or_add_for_write_only_span<float3>(POINTCLOUD_ATTR_POSITION, ATTR_DOMAIN_POINT);
  MutableSpan<float3> point_position_span = point_position.span;

  std::cerr << "joshw: point position span size: " << point_position_span.size() << "\n";
  if (point_position_span.size() != positions->size()) {
    add_customdata_pd(point_cloud, "position", CD_PROP_FLOAT3);
  }

  std::cerr << "joshw: read_points_sample\n";
  read_points_sample(m_schema, sample_sel, config, point_position_span);

  bke::SpanAttributeWriter<float> point_radii =
      point_attributes->lookup_or_add_for_write_only_span<float>(POINTCLOUD_ATTR_RADIUS, ATTR_DOMAIN_POINT);
  MutableSpan<float> point_radii_span = point_radii.span;

  if (radii) {
    for (const int64_t i : point_radii_span.index_range()) {
      point_radii_span[i] = (*radii)[i];
    }
  }
  else {
    point_radii_span.fill(0.01f);
  }

  point_radii.finish();
  point_position.finish();

  read_arbitrary_attributes(config, m_schema, {}, sample_sel, velocity_scale);

  geometry_set.replace_pointcloud(point_cloud);

}

}  // namespace blender::io::alembic
