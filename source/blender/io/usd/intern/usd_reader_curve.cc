/* SPDX-License-Identifier: GPL-2.0-or-later
 * Adapted from the Blender Alembic importer implementation. Copyright 2016 Kévin Dietrich.
 * Modifications Copyright 2021 Tangent Animation. All rights reserved. */

#include "usd_reader_curve.h"

#include "BKE_curves.hh"
#include "BKE_geometry_set.hh"
#include "BKE_mesh.h"
#include "BKE_object.h"
#include "BKE_spline.hh"

#include "BLI_listbase.h"

#include "DNA_curves_types.h"
#include "DNA_object_types.h"

#include "MEM_guardedalloc.h"

#include <pxr/base/vt/array.h>
#include <pxr/base/vt/types.h>
#include <pxr/base/vt/value.h>

#include <pxr/usd/usdGeom/basisCurves.h>
#include <pxr/usd/usdGeom/curves.h>

namespace blender::io::usd {

/** Return the sum of the values of each element in `usdCounts`. This is used for precomputing the
 * total number of points for all curves in some curve primitive. */
static int accumulate_point_count(const pxr::VtIntArray &usdCounts)
{
  int result = 0;
  for (int v : usdCounts) {
    result += v;
  }
  return result;
}

/** Returns true if the number of curves or the number of curve points in each curve differ. */
static bool curves_topology_changed(const CurvesGeometry &geometry,
                                    const pxr::VtIntArray &usdCounts)
{
  if (geometry.curve_num != usdCounts.size()) {
    return true;
  }

  for (int curve_idx = 0; curve_idx < geometry.curve_num; curve_idx++) {
    const int num_in_usd = usdCounts[curve_idx];
    const int num_in_blender = geometry.curve_offsets[curve_idx];

    if (num_in_usd != num_in_blender) {
      return true;
    }
  }

  return false;
}

static CurveType get_curve_type(pxr::TfToken basis)
{
  if (basis == pxr::UsdGeomTokens->bspline) {
    return CURVE_TYPE_NURBS;
  }
  if (basis == pxr::UsdGeomTokens->bezier) {
    return CURVE_TYPE_BEZIER;
  }
  return CURVE_TYPE_POLY;
}

void USDCurvesReader::create_object(Main *bmain, const double /* motionSampleTime */)
{
  curve_ = static_cast<Curves *>(BKE_curves_add(bmain, name_.c_str()));

  object_ = BKE_object_add_only_object(bmain, OB_CURVES, name_.c_str());
  object_->data = curve_;
}

void USDCurvesReader::read_object_data(Main *bmain, double motionSampleTime)
{
  Curves *cu = (Curves *)object_->data;
  read_curve_sample(cu, motionSampleTime);

  if (curve_prim_.GetPointsAttr().ValueMightBeTimeVarying()) {
    add_cache_modifier();
  }

  USDXformReader::read_object_data(bmain, motionSampleTime);
}

void USDCurvesReader::read_curve_sample(Curves *cu, const double motionSampleTime)
{
  curve_prim_ = pxr::UsdGeomBasisCurves(prim_);

  if (!curve_prim_) {
    return;
  }

  pxr::UsdAttribute widthsAttr = curve_prim_.GetWidthsAttr();
  pxr::UsdAttribute vertexAttr = curve_prim_.GetCurveVertexCountsAttr();
  pxr::UsdAttribute pointsAttr = curve_prim_.GetPointsAttr();

  pxr::VtIntArray usdCounts;

  vertexAttr.Get(&usdCounts, motionSampleTime);

  pxr::VtVec3fArray usdPoints;
  pointsAttr.Get(&usdPoints, motionSampleTime);

  pxr::VtFloatArray usdWidths;
  widthsAttr.Get(&usdWidths, motionSampleTime);

  pxr::UsdAttribute basisAttr = curve_prim_.GetBasisAttr();
  pxr::TfToken basis;
  basisAttr.Get(&basis, motionSampleTime);

  pxr::UsdAttribute typeAttr = curve_prim_.GetTypeAttr();
  pxr::TfToken type;
  typeAttr.Get(&type, motionSampleTime);

  pxr::UsdAttribute wrapAttr = curve_prim_.GetWrapAttr();
  pxr::TfToken wrap;
  wrapAttr.Get(&wrap, motionSampleTime);

  bke::CurvesGeometry &geometry = bke::CurvesGeometry::wrap(cu->geometry);
  const int num_subcurves = usdCounts.size();
  const int num_points = accumulate_point_count(usdCounts);

  if (curves_topology_changed(geometry, usdCounts)) {
    geometry.resize(num_points, num_subcurves);
  }

  MutableSpan<int> offsets = geometry.offsets_for_write();
  MutableSpan<float3> positions_ = geometry.positions_for_write();

  const int8_t curve_order = type == pxr::UsdGeomTokens->cubic ? 4 : 2;
  const int default_resolution = 2;

  geometry.curve_types_for_write().fill(get_curve_type(basis));

  if (wrap == pxr::UsdGeomTokens->periodic) {
    geometry.cyclic_for_write().fill(true);
  }

  geometry.nurbs_orders_for_write().fill(curve_order);
  geometry.resolution_for_write().fill(default_resolution);

  int offset = 0;
  for (size_t i = 0; i < num_subcurves; i++) {
    const int num_verts = usdCounts[i];
    offsets[i] = offset;
    offset += num_verts;
  }

  for (const int i_curve : geometry.curves_range()) {
    for (const int i_point : geometry.points_for_curve(i_curve)) {
      positions_[i_point][0] = (float)usdPoints[i_point][0];
      positions_[i_point][1] = (float)usdPoints[i_point][1];
      positions_[i_point][2] = (float)usdPoints[i_point][2];
    }
  }

  if (usdWidths.size()) {
    if (!geometry.radius) {
      geometry.radius = static_cast<float *>(CustomData_add_layer_named(
          &geometry.point_data, CD_PROP_FLOAT, CD_DEFAULT, nullptr, geometry.point_num, "radius"));
    }
    for (const int i_curve : geometry.curves_range()) {
      for (const int i_point : geometry.points_for_curve(i_curve)) {
        geometry.radius[i_point] = usdWidths[i_point];
      }
    }
  }
}

void USDCurvesReader::read_geometry(GeometrySet &geometry_set,
                                    double motionSampleTime,
                                    int /* read_flag */,
                                    const char ** /* err_str */)
{
  if (!curve_prim_) {
    return;
  }

  if (!geometry_set.has_curves()) {
    return;
  }

  Curves *curves = geometry_set.get_curves_for_write();
  read_curve_sample(curves, motionSampleTime);
}

}  // namespace blender::io::usd
