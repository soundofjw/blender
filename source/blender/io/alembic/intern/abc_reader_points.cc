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
  for (size_t i = 0; i < positions->size(); i++) {
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

  if (point_cloud->totpoint != positions->size()) {
    point_cloud = BKE_pointcloud_new_nomain(positions->size());
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

  bke::OutputAttribute_Typed<float3> point_positions =
      component->attribute_try_get_for_output_only<float3>("position", ATTR_DOMAIN_POINT);
  read_points_sample(m_schema, sample_sel, config, point_positions.as_span());
  point_positions.save();

  bke::OutputAttribute_Typed<float> point_radii =
      component->attribute_try_get_for_output_only<float>("radius", ATTR_DOMAIN_POINT);

  MutableSpan<float> point_radii_span = point_radii.as_span();

  if (radii) {
    for (size_t i = 0; i < radii->size(); i++) {
      point_radii_span[i] = (*radii)[i];
    }
  }
  else {
    point_radii_span.fill(0.01f);
  }
  point_radii.save();

  read_arbitrary_attributes(config, m_schema, {}, sample_sel, velocity_scale);

  geometry_set.replace_pointcloud(point_cloud);
}

}  // namespace blender::io::alembic
