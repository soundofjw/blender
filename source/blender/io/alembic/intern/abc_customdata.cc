/* SPDX-License-Identifier: GPL-2.0-or-later
 * Copyright 2016 Kévin Dietrich. All rights reserved. */

/** \file
 * \ingroup balembic
 */

#include "abc_customdata.h"
#include "abc_axis_conversion.h"

#include <Alembic/AbcGeom/All.h>
#include <algorithm>
#include <optional>
#include <unordered_map>

#include "DNA_cachefile_types.h"
#include "DNA_customdata_types.h"
#include "DNA_mesh_types.h"
#include "DNA_meshdata_types.h"
#include "DNA_modifier_types.h"

#include "BLI_assert.h"
#include "BLI_color.hh"
#include "BLI_math_base.h"
#include "BLI_math_bits.h"
#include "BLI_math_vec_types.hh"
#include "BLI_utildefines.h"
#include "BLI_vector.hh"

#include "BLT_translation.h"

#include "BKE_attribute.h"
#include "BKE_customdata.h"
#include "BKE_mesh.h"

/* NOTE: for now only UVs and Vertex Colors are supported for export.
 * Although Alembic only allows for a single UV layer per {I|O}Schema, and does
 * not have a vertex color concept, there is a convention between DCCs to write
 * such data in a way that lets other DCC know what they are for. See comments
 * in the write code for the conventions. */

using namespace Alembic::AbcGeom;

using Alembic::Abc::C4fArraySample;
using Alembic::Abc::UInt32ArraySample;
using Alembic::Abc::V2fArraySample;

namespace blender::io::alembic {

/* ORCO, Generated Coordinates, and Reference Points ("Pref") are all terms for the same thing.
 * Other applications (Maya, Houdini) write these to a property called "Pref". */
static const std::string propNameOriginalCoordinates("Pref");

static void get_uvs(const CDStreamConfig &config,
                    std::vector<Imath::V2f> &uvs,
                    std::vector<uint32_t> &uvidx,
                    const void *cd_data)
{
  const MLoopUV *mloopuv_array = static_cast<const MLoopUV *>(cd_data);

  if (!mloopuv_array) {
    return;
  }

  const int num_poly = config.totpoly;
  MPoly *polygons = config.mpoly;
  MLoop *mloop = config.mloop;

  if (!config.pack_uvs) {
    int count = 0;
    uvidx.resize(config.totloop);
    uvs.resize(config.totloop);

    /* Iterate in reverse order to match exported polygons. */
    for (int i = 0; i < num_poly; i++) {
      MPoly &current_poly = polygons[i];
      const MLoopUV *loopuv = mloopuv_array + current_poly.loopstart + current_poly.totloop;

      for (int j = 0; j < current_poly.totloop; j++, count++) {
        loopuv--;

        uvidx[count] = count;
        uvs[count][0] = loopuv->uv[0];
        uvs[count][1] = loopuv->uv[1];
      }
    }
  }
  else {
    /* Mapping for indexed UVs, deduplicating UV coordinates at vertices. */
    std::vector<std::vector<uint32_t>> idx_map(config.totvert);
    int idx_count = 0;

    for (int i = 0; i < num_poly; i++) {
      MPoly &current_poly = polygons[i];
      MLoop *looppoly = mloop + current_poly.loopstart + current_poly.totloop;
      const MLoopUV *loopuv = mloopuv_array + current_poly.loopstart + current_poly.totloop;

      for (int j = 0; j < current_poly.totloop; j++) {
        looppoly--;
        loopuv--;

        Imath::V2f uv(loopuv->uv[0], loopuv->uv[1]);
        bool found_same = false;

        /* Find UV already in uvs array. */
        for (uint32_t uv_idx : idx_map[looppoly->v]) {
          if (uvs[uv_idx] == uv) {
            found_same = true;
            uvidx.push_back(uv_idx);
            break;
          }
        }

        /* UV doesn't exists for this vertex, add it. */
        if (!found_same) {
          uint32_t uv_idx = idx_count++;
          idx_map[looppoly->v].push_back(uv_idx);
          uvidx.push_back(uv_idx);
          uvs.push_back(uv);
        }
      }
    }
  }
}

const char *get_uv_sample(UVSample &sample, const CDStreamConfig &config, CustomData *data)
{
  const int active_uvlayer = CustomData_get_active_layer(data, CD_MLOOPUV);

  if (active_uvlayer < 0) {
    return "";
  }

  const void *cd_data = CustomData_get_layer_n(data, CD_MLOOPUV, active_uvlayer);

  get_uvs(config, sample.uvs, sample.indices, cd_data);

  return CustomData_get_layer_name(data, CD_MLOOPUV, active_uvlayer);
}

/* Convention to write UVs:
 * - V2fGeomParam on the arbGeomParam
 * - set scope as face varying
 * - (optional due to its behavior) tag as UV using Alembic::AbcGeom::SetIsUV
 */
static void write_uv(const OCompoundProperty &prop,
                     CDStreamConfig &config,
                     const void *data,
                     const char *name)
{
  std::vector<uint32_t> indices;
  std::vector<Imath::V2f> uvs;

  get_uvs(config, uvs, indices, data);

  if (indices.empty() || uvs.empty()) {
    return;
  }

  std::string uv_map_name(name);
  OV2fGeomParam param = config.abc_uv_maps[uv_map_name];

  if (!param.valid()) {
    param = OV2fGeomParam(prop, name, true, kFacevaryingScope, 1);
  }
  OV2fGeomParam::Sample sample(V2fArraySample(&uvs.front(), uvs.size()),
                               UInt32ArraySample(&indices.front(), indices.size()),
                               kFacevaryingScope);
  param.set(sample);
  param.setTimeSampling(config.timesample_index);

  config.abc_uv_maps[uv_map_name] = param;
}

static void get_cols(const CDStreamConfig &config,
                     std::vector<Imath::C4f> &buffer,
                     std::vector<uint32_t> &uvidx,
                     const void *cd_data)
{
  const float cscale = 1.0f / 255.0f;
  const MPoly *polys = config.mpoly;
  const MLoop *mloops = config.mloop;
  const MCol *cfaces = static_cast<const MCol *>(cd_data);

  buffer.reserve(config.totvert);
  uvidx.reserve(config.totvert);

  Imath::C4f col;

  for (int i = 0; i < config.totpoly; i++) {
    const MPoly *p = &polys[i];
    const MCol *cface = &cfaces[p->loopstart + p->totloop];
    const MLoop *mloop = &mloops[p->loopstart + p->totloop];

    for (int j = 0; j < p->totloop; j++) {
      cface--;
      mloop--;

      col[0] = cface->a * cscale;
      col[1] = cface->r * cscale;
      col[2] = cface->g * cscale;
      col[3] = cface->b * cscale;

      buffer.push_back(col);
      uvidx.push_back(buffer.size() - 1);
    }
  }
}

/* Convention to write Vertex Colors:
 * - C3fGeomParam/C4fGeomParam on the arbGeomParam
 * - set scope as vertex varying
 */
static void write_mcol(const OCompoundProperty &prop,
                       CDStreamConfig &config,
                       const void *data,
                       const char *name)
{
  std::vector<uint32_t> indices;
  std::vector<Imath::C4f> buffer;

  get_cols(config, buffer, indices, data);

  if (indices.empty() || buffer.empty()) {
    return;
  }

  std::string vcol_name(name);
  OC4fGeomParam param = config.abc_vertex_colors[vcol_name];

  if (!param.valid()) {
    param = OC4fGeomParam(prop, name, true, kFacevaryingScope, 1);
  }

  OC4fGeomParam::Sample sample(C4fArraySample(&buffer.front(), buffer.size()),
                               UInt32ArraySample(&indices.front(), indices.size()),
                               kVertexScope);

  param.set(sample);
  param.setTimeSampling(config.timesample_index);

  config.abc_vertex_colors[vcol_name] = param;
}

void write_generated_coordinates(const OCompoundProperty &prop, CDStreamConfig &config)
{
  Mesh *mesh = config.mesh;
  const void *customdata = CustomData_get_layer(&mesh->vdata, CD_ORCO);
  if (customdata == nullptr) {
    /* Data not available, so don't even bother creating an Alembic property for it. */
    return;
  }
  const float(*orcodata)[3] = static_cast<const float(*)[3]>(customdata);

  /* Convert 3D vertices from float[3] z=up to V3f y=up. */
  std::vector<Imath::V3f> coords(config.totvert);
  float orco_yup[3];
  for (int vertex_idx = 0; vertex_idx < config.totvert; vertex_idx++) {
    copy_yup_from_zup(orco_yup, orcodata[vertex_idx]);
    coords[vertex_idx].setValue(orco_yup[0], orco_yup[1], orco_yup[2]);
  }

  /* ORCOs are always stored in the normalized 0..1 range in Blender, but Alembic stores them
   * unnormalized, so we need to unnormalize (invert transform) them. */
  BKE_mesh_orco_verts_transform(
      mesh, reinterpret_cast<float(*)[3]>(coords.data()), mesh->totvert, true);

  if (!config.abc_orco.valid()) {
    /* Create the Alembic property and keep a reference so future frames can reuse it. */
    config.abc_orco = OV3fGeomParam(prop, propNameOriginalCoordinates, false, kVertexScope, 1);
  }

  OV3fGeomParam::Sample sample(coords, kVertexScope);
  config.abc_orco.set(sample);
}

void write_custom_data(const OCompoundProperty &prop,
                       CDStreamConfig &config,
                       CustomData *data,
                       int data_type)
{
  eCustomDataType cd_data_type = static_cast<eCustomDataType>(data_type);

  if (!CustomData_has_layer(data, cd_data_type)) {
    return;
  }

  const int active_layer = CustomData_get_active_layer(data, cd_data_type);
  const int tot_layers = CustomData_number_of_layers(data, cd_data_type);

  for (int i = 0; i < tot_layers; i++) {
    const void *cd_data = CustomData_get_layer_n(data, cd_data_type, i);
    const char *name = CustomData_get_layer_name(data, cd_data_type, i);

    if (cd_data_type == CD_MLOOPUV) {
      /* Already exported. */
      if (i == active_layer) {
        continue;
      }

      write_uv(prop, config, cd_data, name);
    }
    else if (cd_data_type == CD_PROP_BYTE_COLOR) {
      write_mcol(prop, config, cd_data, name);
    }
  }
}

/* ************************************************************************** */

using Alembic::Abc::C3fArraySamplePtr;
using Alembic::Abc::C4fArraySamplePtr;
using Alembic::Abc::PropertyHeader;
using Alembic::Abc::UInt32ArraySamplePtr;

/* -------------------------------------------------------------------- */

/* Check the bits and return an eAttrDomain if only one of the bits is active. */
static std::optional<eAttrDomain> valid_domain_or_unknown(uint32_t domain_bits)
{
  if (count_bits_i(domain_bits) == 1) {
    return static_cast<eAttrDomain>(bitscan_forward_uint(domain_bits));
  }

  return {};
}

/* Determine the matching domain given some number of values. The dimensions parameter is used to
 * scale the DomainInfo.length and is meant to be used (i.e. greater than 1) when processing a
 * scalar array for a possible remapping to some n-dimensionnal data type. */
static std::optional<eAttrDomain> matching_domain(const GeometryComponent &geometry_component,
                                                  const int64_t dimensions,
                                                  const int64_t num_values,
                                                  const eAttrDomain requested_domain)
{
  BLI_assert_msg(!ELEM(requested_domain, ATTR_DOMAIN_EDGE, ATTR_DOMAIN_INSTANCE),
                 "received unsupported domain in matching_domain");

  if (requested_domain != ATTR_DOMAIN_AUTO) {
    if (geometry_component.attribute_domain_size(requested_domain) * dimensions != num_values) {
      /* The size of the requested domain does not match what we have, mark as invalid. */
      return {};
    }
    /* Perfect match. */
    return requested_domain;
  }

  /* Use the eAttrDomains as bits in a bitfield, if multiple domains match the data size we
   * will have more than one bit active therefore we cannot determine what domain the data is
   * actually on. We will then delegate to the user the task of telling us through a remapping. */
  uint32_t domain_bits = 0;

  /* Supported domains by Alembic. */
  const eAttrDomain supported_domains[4] = {
      ATTR_DOMAIN_CORNER,
      ATTR_DOMAIN_FACE,
      ATTR_DOMAIN_POINT,
      ATTR_DOMAIN_CURVE,
  };

  for (const eAttrDomain supported_domain : supported_domains) {
    if (geometry_component.attribute_domain_size(supported_domain) * dimensions != num_values) {
      continue;
    }
    domain_bits |= (1 << supported_domain);
  }

  return valid_domain_or_unknown(domain_bits);
}

#if 0
/* Useful for debugging. Turned off to quiet warnings. */
static std::ostream &operator<<(std::ostream &os, eAttrDomain bl_domain)
{
  switch (bl_domain) {
    case ATTR_DOMAIN_POINT:
      os << "POINT";
      break;
    case ATTR_DOMAIN_CORNER:
      os << "CORNER";
      break;
    case ATTR_DOMAIN_FACE:
      os << "FACE";
      break;
    case ATTR_DOMAIN_CURVE:
      os << "CURVE";
      break;
    case ATTR_DOMAIN_EDGE:
      os << "EDGE";
      break;
    case ATTR_DOMAIN_AUTO:
      os << "AUTO";
      break;
    default:
      os << "UNKNOWN";
      break;
  }
  return os;
}
#endif

/* UVs can be defined per-loop (one value per vertex per face), or per-vertex (one value per
 * vertex). The first case is the most common, as this is the standard way of storing this data
 * given that some vertices might be on UV seams and have multiple possible UV coordinates; the
 * second case can happen when the mesh is split according to the UV islands, in which case storing
 * a single UV value per vertex allows to deduplicate data and thus to reduce the file size since
 * vertices are guaranteed to only have a single UV coordinate. */
static bool is_valid_uv_domain(std::optional<eAttrDomain> domain)
{
  return domain.has_value() && ELEM(*domain, ATTR_DOMAIN_CORNER, ATTR_DOMAIN_POINT);
}

static bool is_valid_vertex_color_domain(std::optional<eAttrDomain> domain)
{
  return domain.has_value() && ELEM(*domain, ATTR_DOMAIN_CORNER, ATTR_DOMAIN_POINT);
}

static bool is_valid_domain_for_layer(std::optional<eAttrDomain> domain,
                                      eCustomDataType custom_data_type)
{
  if (!domain.has_value()) {
    return false;
  }

  switch (custom_data_type) {
    case CD_MLOOPUV:
      return is_valid_uv_domain(domain);
    case CD_MCOL:
      return is_valid_vertex_color_domain(domain);
    case CD_ORCO:
      return domain == ATTR_DOMAIN_POINT;
    case CD_PROP_BOOL:
    case CD_PROP_BYTE_COLOR:
    case CD_PROP_COLOR:
    case CD_PROP_FLOAT2:
    case CD_PROP_FLOAT3:
    case CD_PROP_FLOAT:
    case CD_PROP_INT32:
    case CD_PROP_INT8:
      /* These layer types are valid for all domains. */
      return true;
    default:
      /* Unsupported layer type. */
      return false;
  }
}

/* Converts an Alembic scope to a Blender domain. We need to be careful as some Alembic scopes
 * depend on the domain on which they appear. For example, kVaryingScope could mean that the data
 * is varying across the polygons or the vertices. To resolve this, we also use the expected number
 * of elements for each scopes, and the matching domain is returned based on this.
 */
static std::optional<eAttrDomain> to_blender_domain(
    const Alembic::AbcGeom::GeometryScope abc_scope,
    const int64_t element_size,
    const GeometryComponent &geometry_component)
{
  switch (abc_scope) {
    case kConstantScope:
      return matching_domain(geometry_component, 1, element_size, ATTR_DOMAIN_AUTO);
    case kUniformScope:
      /* This should mean one value for the whole object, but some software might use it to mean
       * for example "uniform across the vertices" so we need to try and match this to some domain.
       */
      return matching_domain(geometry_component, 1, element_size, ATTR_DOMAIN_AUTO);
    case kVertexScope:
      /* This is pretty straightforward, just ensure that the sizes match. */
      return matching_domain(geometry_component, 1, element_size, ATTR_DOMAIN_POINT);
    case kFacevaryingScope: {
      /* Fist check that the loops size match, since this is the right domain. */
      std::optional<eAttrDomain> match = matching_domain(
          geometry_component, 1, element_size, ATTR_DOMAIN_CORNER);
      if (match.has_value()) {
        return match;
      }
      /* If not, we may have some data that is face varying, but was written for each vertex
       * instead (e.g. vertex colors). */
      return matching_domain(geometry_component, 1, element_size, ATTR_DOMAIN_POINT);
    }
    case kVaryingScope:
      /* We have a varying field over some domain, which we need to determine. */
      return matching_domain(geometry_component, 1, element_size, ATTR_DOMAIN_AUTO);
    case kUnknownScope:
      return {};
  }

  return {};
}

/* -------------------------------------------------------------------- */

/** \name value_type_converter
 *
 * Utilities to convert values from the types used in Alembic to the ones used in Blender. Besides
 * type conversions, vectors are also converted from Y-up to Z-up.
 * \{ */

template<typename T> struct value_type_converter {
  static bool map_to_bool(const T *value)
  {
    return static_cast<bool>(value[0]);
  }

  static int8_t map_to_int8(const T *value)
  {
    return static_cast<int8_t>(value[0]);
  }

  static int32_t map_to_int32(const T *value)
  {
    return static_cast<int32_t>(value[0]);
  }

  static float map_to_float(const T *value)
  {
    return static_cast<float>(value[0]);
  }

  static float2 map_to_float2(const T *value)
  {
    return {static_cast<float>(value[0]), static_cast<float>(value[1])};
  }

  static float3 map_to_float3(const T *value)
  {
    float3 result{
        static_cast<float>(value[0]), static_cast<float>(value[1]), static_cast<float>(value[2])};
    copy_zup_from_yup(result, result);
    return result;
  }

  static ColorGeometry4f map_to_color_from_rgb(const T *ptr)
  {
    return ColorGeometry4f(
        static_cast<float>(ptr[0]), static_cast<float>(ptr[1]), static_cast<float>(ptr[2]), 1.0f);
  }

  static ColorGeometry4f map_to_color_from_rgba(const T *ptr)
  {
    return ColorGeometry4f(static_cast<float>(ptr[0]),
                           static_cast<float>(ptr[1]),
                           static_cast<float>(ptr[2]),
                           static_cast<float>(ptr[3]));
  }

  static MCol map_to_mcol_from_rgb(const T *ptr)
  {
    MCol mcol;
    mcol.a = unit_float_to_uchar_clamp(static_cast<float>(ptr[0]));
    mcol.r = unit_float_to_uchar_clamp(static_cast<float>(ptr[1]));
    mcol.g = unit_float_to_uchar_clamp(static_cast<float>(ptr[2]));
    mcol.b = 255;
    return mcol;
  }

  static MCol map_to_mcol_from_rgba(const T *ptr)
  {
    MCol mcol;
    mcol.a = unit_float_to_uchar_clamp(static_cast<float>(ptr[0]));
    mcol.r = unit_float_to_uchar_clamp(static_cast<float>(ptr[1]));
    mcol.g = unit_float_to_uchar_clamp(static_cast<float>(ptr[2]));
    mcol.b = unit_float_to_uchar_clamp(static_cast<float>(ptr[3]));
    return mcol;
  }
};

template<> struct value_type_converter<Imath::V3f> {
  static float3 convert_value(const Imath::V3f &v)
  {
    return v.getValue();
  }
};

template<> struct value_type_converter<Imath::V3d> {
  static float3 convert_value(const Imath::V3d &v)
  {
    const double *ptr = v.getValue();
    return float3(
        static_cast<float>(ptr[0]), static_cast<float>(ptr[1]), static_cast<float>(ptr[2]));
  }
};

template<> struct value_type_converter<Imath::C3f> {
  static MCol convert_value(const Imath::C3f &v)
  {
    MCol mcol;
    mcol.a = unit_float_to_uchar_clamp(static_cast<float>(v[0]));
    mcol.r = unit_float_to_uchar_clamp(static_cast<float>(v[1]));
    mcol.g = unit_float_to_uchar_clamp(static_cast<float>(v[2]));
    mcol.b = 255;
    return mcol;
  }
};

template<> struct value_type_converter<Imath::C4f> {
  static MCol convert_value(const Imath::C4f &v)
  {
    MCol mcol;
    mcol.a = unit_float_to_uchar_clamp(static_cast<float>(v[0]));
    mcol.r = unit_float_to_uchar_clamp(static_cast<float>(v[1]));
    mcol.g = unit_float_to_uchar_clamp(static_cast<float>(v[2]));
    mcol.b = unit_float_to_uchar_clamp(static_cast<float>(v[3]));
    return mcol;
  }
};

template<typename T> struct to_scalar_type {
  using type = T;
};

template<> struct to_scalar_type<Imath::V2f> {
  using type = float;
};

template<> struct to_scalar_type<Imath::V3f> {
  using type = float;
};

template<> struct to_scalar_type<Imath::V3d> {
  using type = double;
};

template<> struct to_scalar_type<Imath::C3f> {
  using type = float;
};

template<> struct to_scalar_type<Imath::C4f> {
  using type = float;
};

/** \} */

/* -------------------------------------------------------------------- */

static size_t mcols_out_of_bounds_check(const size_t color_index,
                                        const size_t array_size,
                                        const std::string &iobject_full_name,
                                        const std::string &prop_name,
                                        bool &r_is_out_of_bounds,
                                        bool &r_bounds_warning_given)
{
  if (color_index < array_size) {
    return color_index;
  }

  if (!r_bounds_warning_given) {
    std::cerr << "Alembic: color index out of bounds "
                 "reading face colors for object "
              << iobject_full_name << ", property " << prop_name << std::endl;
    r_bounds_warning_given = true;
  }
  r_is_out_of_bounds = true;
  return 0;
}

/* -------------------------------------------------------------------- */

/** \name AbcAttributeMapping
 *
 * Mirror of the mapping enumeration in DNA_cachefile_types.h. It is more specific and used for the
 * final mapping of all attributes even those without a user supplied mapping. It there is a user
 * supplied mapping, this will also replace potential default values from #CacheAttributeMapping
 * with their resolved final values (e.g. a mapping set to `CACHEFILE_MAP_NONE` will have a proper
 * domain set).
 * \{ */

struct AbcAttributeMapping {
  eCustomDataType type;
  /* This might be invalid if we cannot determine the domain. */
  eAttrDomain domain;
  /* This is to differentiate between RGB and RGBA colors. */
  bool is_rgba = false;
};

/* Try to resolve the mapping for an attribute from the number of individual elements in the array
 * sample and the mapping as desired by the user. */
static std::optional<AbcAttributeMapping> final_mapping_from_cache_mapping(
    const CacheAttributeMapping &mapping,
    const GeometryComponent &geometry_component,
    const int64_t num_elements)
{
  BLI_assert_msg(num_elements > 0, "final_mapping_from_cache_mapping() needs data to work on");

  const eAttrDomain mapping_domain = static_cast<eAttrDomain>(mapping.domain);

  switch (mapping.mapping) {
    case CACHEFILE_ATTRIBUTE_MAP_NONE:
      return {};
    case CACHEFILE_ATTRIBUTE_MAP_TO_UVS: {
      /* UVs have 2 values per element. */
      const std::optional<eAttrDomain> domain_hint = matching_domain(
          geometry_component, 2, num_elements, mapping_domain);
      if (!is_valid_uv_domain(domain_hint)) {
        return {};
      }
      return AbcAttributeMapping{CD_MLOOPUV, *domain_hint};
    }
    case CACHEFILE_ATTRIBUTE_MAP_TO_BYTE_COLOR: {
      /* 3 values for RGB, 4 for RGBA */
      std::optional<eAttrDomain> rgb_domain_hint = matching_domain(
          geometry_component, 3, num_elements, mapping_domain);
      std::optional<eAttrDomain> rgba_domain_hint = matching_domain(
          geometry_component, 4, num_elements, mapping_domain);

      const bool is_rgb = is_valid_vertex_color_domain(rgb_domain_hint);
      const bool is_rgba = is_valid_vertex_color_domain(rgba_domain_hint);

      if (is_rgb && !is_rgba) {
        return AbcAttributeMapping{CD_MCOL, *rgb_domain_hint, false};
      }

      if (!is_rgb && is_rgba) {
        return AbcAttributeMapping{CD_MCOL, *rgba_domain_hint, true};
      }
      /* Either both are unknown, or we matched two different domains in which case we cannot
       * resolve the ambiguity. */
      return {};
    }
    case CACHEFILE_ATTRIBUTE_MAP_TO_FLOAT2: {
      std::optional<eAttrDomain> r_domain_hint = matching_domain(
          geometry_component, 2, num_elements, mapping_domain);
      if (!r_domain_hint.has_value()) {
        return {};
      }
      return AbcAttributeMapping{CD_PROP_FLOAT2, *r_domain_hint};
    }
    case CACHEFILE_ATTRIBUTE_MAP_TO_FLOAT3: {
      std::optional<eAttrDomain> r_domain_hint = matching_domain(
          geometry_component, 3, num_elements, mapping_domain);
      if (!r_domain_hint.has_value()) {
        return {};
      }
      return AbcAttributeMapping{CD_PROP_FLOAT3, *r_domain_hint};
    }
    case CACHEFILE_ATTRIBUTE_MAP_TO_COLOR: {
      /* 3 values for RGB, 4 for RGBA */
      std::optional<eAttrDomain> rgb_domain_hint = matching_domain(
          geometry_component, 3, num_elements, mapping_domain);
      std::optional<eAttrDomain> rgba_domain_hint = matching_domain(
          geometry_component, 4, num_elements, mapping_domain);

      if (!rgb_domain_hint.has_value() && rgba_domain_hint.has_value()) {
        return AbcAttributeMapping{CD_PROP_COLOR, *rgba_domain_hint, true};
      }

      if (rgb_domain_hint.has_value() && !rgba_domain_hint.has_value()) {
        return AbcAttributeMapping{CD_PROP_COLOR, *rgb_domain_hint, false};
      }

      /* Either both are unknown, or we matched two different domains in which case we cannot
       * resolve the ambiguity. */
      return {};
    }
  }

  return {};
}

/** \} */

/* -------------------------------------------------------------------- */

static bool can_add_custom_data_layer(const CustomData *data,
                                      eCustomDataType type,
                                      int max_allowed_layers)
{
  if (data == nullptr) {
    return false;
  }

  return CustomData_number_of_layers(data, type) < max_allowed_layers;
}

/* Check if we do not already have reached the maximum allowed number of vertex colors layers. */
static bool can_add_vertex_color_layer(const CDStreamConfig &config)
{
  if (config.mesh == nullptr) {
    return false;
  }
  return true;  // joshw: no max mcol?
}

/* Check if we do not already have reached the maximum allowed number of UV layers. */
static bool can_add_uv_layer(const CDStreamConfig &config)
{
  if (config.mesh == nullptr) {
    return false;
  }
  return can_add_custom_data_layer(&config.mesh->ldata, CD_MLOOPUV, MAX_MTFACE);
}

/* For converting attributes with a loop domain, we need to convert the polygon winding order as
 * well, as viewed from Blender, Alembic orders vertices around a polygon in reverse.
 * The callback will be called for each loop, and the index passed to it will be the index of the
 * data in the source_domain. If source_domain is POINT, the passed index will be that of the point
 * of the corresponding loop vertex. If it is CORNER, it will be that of the loop. For now, we do
 * not support FACE domain as a source_domain.
 */
template<typename BlenderType, typename Callback>
static void iterate_attribute_loop_domain(const CDStreamConfig &config,
                                          MutableSpan<BlenderType> varray,
                                          eAttrDomain source_domain,
                                          Callback callback)
{
  if (!ELEM(source_domain, ATTR_DOMAIN_CORNER, ATTR_DOMAIN_POINT)) {
    return;
  }

  Mesh *mesh = config.mesh;
  if (mesh == nullptr) {
    std::cerr << "Mesh is missing during Alembic import.\n";
  }
  MPoly *mpolys = mesh->mpoly;
  MLoop *mloops = mesh->mloop;
  unsigned int loop_index, rev_loop_index;
  const bool index_per_loop = source_domain == ATTR_DOMAIN_CORNER;

  for (int i = 0; i < mesh->totpoly; i++) {
    MPoly &poly = mpolys[i];
    unsigned int rev_loop_offset = poly.loopstart + poly.totloop - 1;

    for (int f = 0; f < poly.totloop; f++) {
      rev_loop_index = rev_loop_offset - f;
      loop_index = index_per_loop ? poly.loopstart + f : mloops[rev_loop_index].v;
      varray[rev_loop_index] = callback(loop_index);
    }
  }
}

static CustomDataLayer *ensure_attribute_on_domain(ID *id,
                                                   const char *name,
                                                   const int cd_type,
                                                   const eAttrDomain domain)
{
  CustomDataLayer *layer = BKE_id_attribute_find(id, name, cd_type, domain);

  if (layer) {
    return layer;
  }

  /* Check if an existing attribute exists on any other domain, and if so, remove it. */
  for (int domain_iter = ATTR_DOMAIN_POINT; domain_iter < ATTR_DOMAIN_NUM; domain_iter++) {
    if (domain_iter == domain) {
      /* This was just checked above, no need to reprocess. */
      continue;
    }
    layer = BKE_id_attribute_find(id, name, cd_type, static_cast<eAttrDomain>(domain_iter));
    if (layer) {
      BKE_id_attribute_remove(id, name, nullptr);
      break;
    }
  }

  return BKE_id_attribute_new(id, name, cd_type, domain, nullptr);
}

static void *add_customdata_cb(Mesh *mesh, const char *name, int data_type)
{
  eCustomDataType cd_data_type = static_cast<eCustomDataType>(data_type);
  void *cd_ptr;
  CustomData *loopdata;
  int numloops;

  /* unsupported custom data type -- don't do anything. */
  if (!ELEM(cd_data_type, CD_MLOOPUV)) {
    return NULL;
  }

  loopdata = &mesh->ldata;
  cd_ptr = CustomData_get_layer_named(loopdata, cd_data_type, name);
  if (cd_ptr != NULL) {
    /* layer already exists, so just return it. */
    return cd_ptr;
  }

  /* Create a new layer. */
  numloops = mesh->totloop;
  cd_ptr = CustomData_add_layer_named(
      loopdata, cd_data_type, CD_SET_DEFAULT, NULL, numloops, name);
  return cd_ptr;
}

/* Main entry point for creating a custom data layer from an Alembic attribute.
 *
 * The callback is responsible for filling the custom data layer, one value at a time. Its
 * signature should be : BlenderType(size_t). The parameter is the current index of the iteration,
 * and is dependant on the target_domain.
 *
 * The source_domain is the resolved domain for the Alembic data, while the target_domain is the
 * domain of the final Blender data. We split these as they can be different either due to
 * remapping, or because we have an attribute that is always on one domain in Blender, but can be
 * expressed in multiple scopes in Alembic (e.g. UV maps are loop domain in Blender, but loop or
 * vertex in Alembic).
 */
template<typename BlenderType, typename Callback>
static void create_layer_for_domain(const CDStreamConfig &config,
                                    const eAttrDomain source_domain,
                                    const eAttrDomain target_domain,
                                    const eCustomDataType cd_type,
                                    const StringRefNull name,
                                    const Callback callback)
{
  BLI_assert(source_domain != ATTR_DOMAIN_AUTO);
  BLI_assert(target_domain != ATTR_DOMAIN_AUTO);

  GeometryComponent &geometry_component = *config.geometry_component;
  /* These layer types cannot be created via the geometry component. */
  if (ELEM(cd_type, CD_ORCO, CD_MLOOPUV)) {
    /*CustomDataLayer *layer = ensure_attribute_on_domain(
        &config.mesh->id, name.c_str(), cd_type, target_domain);*/
    void *data = add_customdata_cb(config.mesh, name.c_str(), cd_type);

    BlenderType *layer_data = static_cast<BlenderType *>(data);
    MutableSpan<BlenderType> span = MutableSpan(
        layer_data, geometry_component.attribute_domain_size(target_domain));

    if (target_domain == ATTR_DOMAIN_CORNER) {
      iterate_attribute_loop_domain(config, span, source_domain, callback);
      return;
    }

    /* Other domains can be simply iterated. */
    for (const int64_t i : span.index_range()) {
      span[i] = callback(i);
    }
  }
  else {
    bke::AttributeIDRef attribute_id(name);

    /* Try to remove any attribute with the same name. It might have been loaded
     * on a different domain or with a different data type (e.g. if vectors are
     * flattened during exports). */
    if (geometry_component.attributes()->contains(attribute_id)) {
      if (!geometry_component.attributes_for_write()->remove(attribute_id)) {
        return;
      }
    }

    bke::SpanAttributeWriter<BlenderType> write_attribute =
        geometry_component.attributes_for_write()->lookup_or_add_for_write_only_span<BlenderType>(
            attribute_id, target_domain);
    /* We just created the attribute, it should exist. */
    BLI_assert(write_attribute);

    if (target_domain == ATTR_DOMAIN_CORNER) {
      iterate_attribute_loop_domain(config, write_attribute.span, source_domain, callback);
      write_attribute.finish();
      return;
    }

    /* Other domains can be simply iterated. */
    for (const int64_t i : write_attribute.span.index_range()) {
      write_attribute.span[i] = callback(i);
    }

    write_attribute.finish();
  }
}

/* Wrapper around create_layer_for_domain with similar source and target domains. */
template<typename BlenderType, typename Callback>
static void create_layer_for_domain(const CDStreamConfig &config,
                                    const eAttrDomain bl_domain,
                                    const eCustomDataType cd_type,
                                    const StringRefNull name,
                                    const Callback callback)
{
  return create_layer_for_domain<BlenderType>(
      config, bl_domain, bl_domain, cd_type, name, callback);
}

/* Wrapper around create_layer_for_domain with the loop domain being the target domain. */
template<typename BlenderType, typename Callback>
static void create_loop_layer_for_domain(const CDStreamConfig &config,
                                         const eAttrDomain source_domain,
                                         const eCustomDataType cd_type,
                                         const StringRefNull name,
                                         const Callback callback)
{
  return create_layer_for_domain<BlenderType>(
      config, source_domain, ATTR_DOMAIN_CORNER, cd_type, name, callback);
}

enum class AbcAttributeReadError {
  /* Default value for success. */
  READ_SUCCESS,
  /* We do not support this attribute type. */
  UNSUPPORTED_TYPE,
  /* The attribute is invalid (e.g. corrupt file or data). */
  INVALID_ATTRIBUTE,
  /* We cannot determine the domain for the attribute, either from mismatching element size, or
   * ambiguous domain used in the Alembic archive. */
  DOMAIN_RESOLUTION_FAILED,
  /* Scope resolution succeeded, but the domain is not valid for the data. */
  INVALID_DOMAIN,
  /* The mapping selected by the user is not possible. */
  MAPPING_IMPOSSIBLE,
  /* The limit of attributes for the CustomData layer is reached (this should only concern UVs and
   * vertex colors). */
  TOO_MANY_UVS_ATTRIBUTES,
  TOO_MANY_VCOLS_ATTRIBUTES,
};

static eCustomDataType custom_data_type_for_pod(PlainOldDataType pod_type, uint extent)
{
  switch (pod_type) {
    case kBooleanPOD:
      return CD_PROP_BOOL;
    case kUint8POD:
    case kInt8POD:
      return CD_PROP_INT8;
    case kUint16POD:
    case kInt16POD:
    case kUint32POD:
    case kInt32POD:
      return CD_PROP_INT32;
    case kFloat32POD:
    case kFloat64POD: {
      if (extent == 2) {
        return CD_PROP_FLOAT2;
      }

      if (extent == 3) {
        return CD_PROP_FLOAT3;
      }

      return CD_PROP_FLOAT;
    }
    /* These are unsupported for now. */
    case kFloat16POD:
    case kUint64POD:
    case kInt64POD:
    case kStringPOD:
    case kWstringPOD:
    default:
      /* Use this as invalid value. */
      return CD_AUTO_FROM_NAME;
  }
}

template<typename TRAIT>
static eCustomDataType get_default_custom_data_type(const CDStreamConfig &config,
                                                    const ITypedGeomParam<TRAIT> &param,
                                                    bool &r_is_rgba)
{
  if (std::is_same_v<ITypedGeomParam<TRAIT>, IC3fGeomParam>) {
    if (!can_add_vertex_color_layer(config)) {
      /* CD_MCOL is full, fallback to generic colors. */
      return CD_PROP_COLOR;
    }

    return CD_MCOL;
  }

  if (std::is_same_v<ITypedGeomParam<TRAIT>, IC4fGeomParam>) {
    r_is_rgba = true;
    if (!can_add_vertex_color_layer(config)) {
      /* CD_MCOL is full, fallback to generic colors. */
      return CD_PROP_COLOR;
    }

    return CD_MCOL;
  }

  if (std::is_same_v<ITypedGeomParam<TRAIT>, IV2fGeomParam>) {
    if (Alembic::AbcGeom::isUV(param.getHeader())) {
      if (can_add_uv_layer(config)) {
        return CD_MLOOPUV;
      }
    }

    /* If it is not a UV map or if CD_MLOOPUV is full, fallback to generic 2D vectors. */
    return CD_PROP_FLOAT2;
  }

  /* Ensure UVs are also detected if written as doubles. */
  if (std::is_same_v<ITypedGeomParam<TRAIT>, IV2dGeomParam>) {
    if (Alembic::AbcGeom::isUV(param.getHeader())) {
      if (can_add_uv_layer(config)) {
        return CD_MLOOPUV;
      }
    }

    /* If it is not a UV map or if CD_MLOOPUV is full, fallback to generic 2D vectors. */
    return CD_PROP_FLOAT2;
  }

  if (param.getName() == propNameOriginalCoordinates) {
    return CD_ORCO;
  }

  const DataType &data_type = param.getDataType();
  return custom_data_type_for_pod(data_type.getPod(), data_type.getExtent());
}

template<typename TRAIT>
static std::optional<AbcAttributeMapping> determine_attribute_mapping(
    const CDStreamConfig &config,
    const ITypedGeomParam<TRAIT> &param,
    int64_t num_values,
    const CacheAttributeMapping *desired_mapping)
{
  if (desired_mapping) {
    auto opt_final_mapping = final_mapping_from_cache_mapping(*desired_mapping,
                                                              *config.geometry_component,
                                                              num_values *
                                                                  param.getDataType().getExtent());

    /* If we cannot apply the user defined remapping, let's still try to load it with default
     * parameters. */
    if (opt_final_mapping.has_value()) {
      return opt_final_mapping;
    }
  }

  const std::optional<eAttrDomain> default_domain = to_blender_domain(
      param.getScope(), num_values, *config.geometry_component);

  if (!default_domain.has_value()) {
    /* We cannot by default map this attribute, most likely a user defined mapping will be
     * required. */
    return {};
  }

  AbcAttributeMapping default_mapping;
  default_mapping.domain = *default_domain;
  default_mapping.type = get_default_custom_data_type(config, param, default_mapping.is_rgba);

  if (!is_valid_domain_for_layer(default_mapping.domain, default_mapping.type)) {
    return {};
  }

  return default_mapping;
}

template<typename TRAIT>
static AbcAttributeReadError process_typed_attribute(const CDStreamConfig &config,
                                                     const CacheAttributeMapping *mapping,
                                                     const ITypedGeomParam<TRAIT> &param,
                                                     ISampleSelector iss,
                                                     const float velocity_scale)
{
  typename ITypedGeomParam<TRAIT>::Sample sample;
  param.getIndexed(sample, iss);

  if (!sample.valid()) {
    return AbcAttributeReadError::INVALID_ATTRIBUTE;
  }

  const AttributeReadingHelper &attr_helper = *config.attribute_helper;

  const TypedArraySample<TRAIT> &values = *sample.getVals();
  const UInt32ArraySamplePtr indices = sample.getIndices();
  const size_t num_values = indices->size() > 0 ? indices->size() : values.size();

  std::optional<AbcAttributeMapping> opt_abc_mapping = determine_attribute_mapping(
      config, param, static_cast<int64_t>(num_values), mapping);

  if (!opt_abc_mapping.has_value()) {
    return AbcAttributeReadError::MAPPING_IMPOSSIBLE;
  }

  AbcAttributeMapping abc_mapping = *opt_abc_mapping;
  eAttrDomain domain = abc_mapping.domain;

  if (!attr_helper.should_read_attribute(param.getName())) {
    return AbcAttributeReadError::READ_SUCCESS;
  }

  using abc_type = typename TRAIT::value_type;
  using abc_scalar_type = typename to_scalar_type<abc_type>::type;

  const abc_scalar_type *input_data = reinterpret_cast<const abc_scalar_type *>(values.get());

  switch (abc_mapping.type) {
    default: {
      return AbcAttributeReadError::MAPPING_IMPOSSIBLE;
    }
    case CD_PROP_BOOL:
      create_layer_for_domain<bool>(config, domain, CD_PROP_BOOL, param.getName(), [&](size_t i) {
        return value_type_converter<abc_scalar_type>::map_to_bool(&input_data[i]);
      });
      return AbcAttributeReadError::READ_SUCCESS;
    case CD_PROP_INT8:
      create_layer_for_domain<int8_t>(
          config, domain, CD_PROP_INT8, param.getName(), [&](size_t i) {
            return value_type_converter<abc_scalar_type>::map_to_int8(&input_data[i]);
          });
      return AbcAttributeReadError::READ_SUCCESS;
    case CD_PROP_INT32:
      create_layer_for_domain<int32_t>(
          config, domain, CD_PROP_INT32, param.getName(), [&](size_t i) {
            return value_type_converter<abc_scalar_type>::map_to_int32(&input_data[i]);
          });
      return AbcAttributeReadError::READ_SUCCESS;
    case CD_PROP_FLOAT:
      create_layer_for_domain<float>(
          config, domain, CD_PROP_FLOAT, param.getName(), [&](size_t i) {
            return value_type_converter<abc_scalar_type>::map_to_float(&input_data[i]);
          });
      return AbcAttributeReadError::READ_SUCCESS;
    case CD_PROP_FLOAT2:
      create_layer_for_domain<float2>(
          config, domain, CD_PROP_FLOAT2, param.getName(), [&](size_t i) {
            return value_type_converter<abc_scalar_type>::map_to_float2(&input_data[i * 2]);
          });
      return AbcAttributeReadError::READ_SUCCESS;
    case CD_ORCO: {
      if (!attr_helper.original_coordinates_requested()) {
        return AbcAttributeReadError::READ_SUCCESS;
      }

      /* Check that we indeed have an attribute on the points. */
      if (domain != ATTR_DOMAIN_POINT) {
        return AbcAttributeReadError::INVALID_DOMAIN;
      }

      create_layer_for_domain<float3>(
          config, domain, abc_mapping.type, param.getName(), [&](size_t i) {
            const float3 value = value_type_converter<abc_scalar_type>::map_to_float3(
                &input_data[i * 3]);
            return value;
          });

      Mesh *mesh = config.mesh;
      void *customdata = CustomData_get_layer(&mesh->vdata, CD_ORCO);
      float(*orcodata)[3] = static_cast<float(*)[3]>(customdata);

      /* ORCOs are always stored in the normalized 0..1 range in Blender, but Alembic stores them
       * unnormalized, so we need to normalize them (invert = false). */
      BKE_mesh_orco_verts_transform(
          mesh, reinterpret_cast<float(*)[3]>(&orcodata[0]), mesh->totvert, false);

      return AbcAttributeReadError::READ_SUCCESS;
    }
    case CD_PROP_FLOAT3: {
      std::string param_name = param.getName();
      bool is_velocity = false;
      if (param.getName() == attr_helper.velocity_attribute_name()) {
        param_name = "velocity";
        is_velocity = true;

        /* Check that we indeed have an attribute on the points. */
        if (domain != ATTR_DOMAIN_POINT) {
          return AbcAttributeReadError::INVALID_DOMAIN;
        }
      }

      create_layer_for_domain<float3>(config, domain, abc_mapping.type, param_name, [&](size_t i) {
        const float3 value = value_type_converter<abc_scalar_type>::map_to_float3(
            &input_data[i * 3]);
        return is_velocity ? value * velocity_scale : value;
      });
      return AbcAttributeReadError::READ_SUCCESS;
    }
    case CD_PROP_COLOR: {
      if (abc_mapping.is_rgba) {
        create_layer_for_domain<ColorGeometry4f>(
            config, domain, CD_PROP_COLOR, param.getName(), [&](size_t i) {
              return value_type_converter<abc_scalar_type>::map_to_color_from_rgba(
                  &input_data[i * 4]);
            });
      }
      else {
        create_layer_for_domain<ColorGeometry4f>(
            config, domain, CD_PROP_COLOR, param.getName(), [&](size_t i) {
              return value_type_converter<abc_scalar_type>::map_to_color_from_rgb(
                  &input_data[i * 3]);
            });
      }
      return AbcAttributeReadError::READ_SUCCESS;
    }
    case CD_MLOOPUV: {
      if (!can_add_uv_layer(config)) {
        return AbcAttributeReadError::TOO_MANY_UVS_ATTRIBUTES;
      }

      create_loop_layer_for_domain<float2>(
          config, domain, CD_MLOOPUV, param.getName(), [&](size_t i) {
            i = indices->size() ? (*indices)[i] : i;
            return value_type_converter<abc_scalar_type>::map_to_float2(&input_data[i * 2]);
          });

      return AbcAttributeReadError::READ_SUCCESS;
    }
    case CD_MCOL: {
      if (!can_add_vertex_color_layer(config)) {
        return AbcAttributeReadError::TOO_MANY_VCOLS_ATTRIBUTES;
      }

      const bool is_facevarying = domain == ATTR_DOMAIN_CORNER;

      /* Read the vertex colors */
      bool bounds_warning_given = false;

      /* The colors can go through two layers of indexing. Often the 'indices'
       * array doesn't do anything (i.e. indices[n] = n), but when it does, it's
       * important. Blender 2.79 writes indices incorrectly (see T53745), which
       * is why we have to check for indices->size() > 0 */
      bool use_dual_indexing = is_facevarying && indices->size() > 0;

      create_loop_layer_for_domain<MCol>(
          config, domain, CD_PROP_BYTE_COLOR, param.getName(), [&](size_t loop_index) {
            size_t color_index = loop_index;
            if (use_dual_indexing) {
              color_index = (*indices)[color_index];
            }

            bool is_mcols_out_of_bounds = false;
            color_index = mcols_out_of_bounds_check(color_index,
                                                    values.size(),
                                                    config.iobject_full_name,
                                                    param.getName(),
                                                    is_mcols_out_of_bounds,
                                                    bounds_warning_given);

            if (is_mcols_out_of_bounds) {
              return MCol{};
            }

            if (abc_mapping.is_rgba) {
              return value_type_converter<abc_scalar_type>::map_to_mcol_from_rgba(
                  &input_data[loop_index * 4]);
            }

            return value_type_converter<abc_scalar_type>::map_to_mcol_from_rgb(
                &input_data[loop_index * 3]);
          });

      return AbcAttributeReadError::READ_SUCCESS;
    }
  }
  return AbcAttributeReadError::READ_SUCCESS;
}

static AbcAttributeReadError read_mesh_uvs(const CDStreamConfig &config,
                                           const IV2fGeomParam &uv_param,
                                           ISampleSelector sample_sel)
{
  BLI_assert(config.mesh);

  if (!uv_param.valid() || !uv_param.isIndexed()) {
    return AbcAttributeReadError::INVALID_ATTRIBUTE;
  }

  IV2fGeomParam::Sample sample;
  uv_param.getIndexed(sample, sample_sel);

  const Alembic::AbcGeom::V2fArraySamplePtr &uvs = sample.getVals();
  const Alembic::AbcGeom::UInt32ArraySamplePtr &indices = sample.getIndices();

  const std::optional<eAttrDomain> bl_domain = to_blender_domain(
      uv_param.getScope(), static_cast<int64_t>(indices->size()), *config.geometry_component);

  if (!is_valid_uv_domain(bl_domain)) {
    return AbcAttributeReadError::INVALID_DOMAIN;
  }

  /* According to the convention, primary UVs should have had their name set using
   * Alembic::Abc::SetSourceName. If there is no such name, use the name defined for the uv_param.
   */
  std::string name = Alembic::Abc::GetSourceName(uv_param.getMetaData());
  if (name.empty()) {
    name = uv_param.getName();
  }

  create_loop_layer_for_domain<MLoopUV>(
      config, *bl_domain, CD_MLOOPUV, name, [&](size_t loop_index) {
        size_t uv_index = (*indices)[loop_index];
        const Imath::V2f &uv = (*uvs)[uv_index];

        MLoopUV result;
        result.uv[0] = uv[0];
        result.uv[1] = uv[1];
        return result;
      });

  return AbcAttributeReadError::READ_SUCCESS;
}

/* This structure holds data for a requested attribute on the Alembic object. Since attributes can
 * be either in the `.arbGeomParams` or on the schema, we use this to centralize data about
 * attributes and simplify data extraction with a common interface. */
struct ParsedAttributeDesc {
  ICompoundProperty parent;
  const PropertyHeader &prop_header;
  const CacheAttributeMapping *mapping;
};

/* Extract supported attributes from the ICompoundProperty, and associate them with any mapping
 * with a matching name. This does not verify yet that the mapping is valid, it will be done during
 * data processing. */
static Vector<ParsedAttributeDesc> parse_attributes(const AttributeReadingHelper &attribute_helper,
                                                    const ICompoundProperty &arb_geom_params)
{
  Vector<ParsedAttributeDesc> result;
  if (!arb_geom_params.valid()) {
    return result;
  }

  for (size_t i = 0; i < arb_geom_params.getNumProperties(); ++i) {
    const PropertyHeader &prop = arb_geom_params.getPropertyHeader(i);

    /* Blender does not support scalar (object domain) attributes at the moment. */
    if (prop.isScalar()) {
      continue;
    }

    const CacheAttributeMapping *mapping = attribute_helper.get_mapping(prop.getName());
    result.append({arb_geom_params, prop, mapping});
  }

  return result;
}

/* This function should be used for any attribute processing to ensure that all supported attribute
 * types are handled.
 *
 * Unsupported attribute types are:
 * - int64 and uint64, since we may have data loss as Blender only supports int32
 * - half floating points (scalar, vec2, vec3, color)
 * - matrices (3x3, 4x4)
 * - string
 * - quaternions
 * - 2d normals
 * - 2d & 3d bounding boxes
 */
template<typename OpType>
static auto abc_attribute_type_operation(const ICompoundProperty &parent_prop,
                                         const PropertyHeader &prop,
                                         OpType &&op)
{
  if (IFloatGeomParam::matches(prop)) {
    IFloatGeomParam param = IFloatGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IDoubleGeomParam::matches(prop)) {
    IDoubleGeomParam param = IDoubleGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IBoolGeomParam::matches(prop)) {
    IBoolGeomParam param = IBoolGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (ICharGeomParam::matches(prop)) {
    ICharGeomParam param = ICharGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IInt16GeomParam::matches(prop)) {
    IInt16GeomParam param = IInt16GeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IInt32GeomParam::matches(prop)) {
    IInt32GeomParam param = IInt32GeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IUcharGeomParam::matches(prop)) {
    IUcharGeomParam param = IUcharGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IUInt16GeomParam::matches(prop)) {
    IUInt16GeomParam param = IUInt16GeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IUInt32GeomParam::matches(prop)) {
    IUInt32GeomParam param = IUInt32GeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IV2fGeomParam::matches(prop)) {
    IV2fGeomParam param = IV2fGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IN3fGeomParam::matches(prop)) {
    IN3fGeomParam param = IN3fGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IN3dGeomParam::matches(prop)) {
    IN3dGeomParam param = IN3dGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IP3fGeomParam::matches(prop)) {
    IP3fGeomParam param = IP3fGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IP3dGeomParam::matches(prop)) {
    IP3dGeomParam param = IP3dGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IV3fGeomParam::matches(prop)) {
    IV3fGeomParam param = IV3fGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IV3dGeomParam::matches(prop)) {
    IV3dGeomParam param = IV3dGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IC3fGeomParam::matches(prop)) {
    IC3fGeomParam param = IC3fGeomParam(parent_prop, prop.getName());
    return op(param);
  }
  if (IC4fGeomParam::matches(prop)) {
    IC4fGeomParam param = IC4fGeomParam(parent_prop, prop.getName());
    return op(param);
  }

  return op.default_handler_for_unsupported_attribute_type(prop.getName());
}

static const char *get_mapping_string(const CacheAttributeMapping *mapping)
{
  switch (mapping->mapping) {
    case CACHEFILE_ATTRIBUTE_MAP_NONE:
      return "default";
    case CACHEFILE_ATTRIBUTE_MAP_TO_UVS:
      return "uvs";
    case CACHEFILE_ATTRIBUTE_MAP_TO_COLOR:
      return "color";
    case CACHEFILE_ATTRIBUTE_MAP_TO_BYTE_COLOR:
      return "byte color";
    case CACHEFILE_ATTRIBUTE_MAP_TO_FLOAT2:
      return "float2";
    case CACHEFILE_ATTRIBUTE_MAP_TO_FLOAT3:
      return "float3";
  }

  return "unknown";
}

static const char *get_domain_string(const CacheAttributeMapping *mapping)
{
  switch (static_cast<eAttrDomain>(mapping->domain)) {
    case ATTR_DOMAIN_AUTO:
      return "auto";
    case ATTR_DOMAIN_POINT:
      return "point";
    case ATTR_DOMAIN_EDGE:
      return "edge";
    case ATTR_DOMAIN_FACE:
      return "face";
    case ATTR_DOMAIN_CORNER:
      return "face corner";
    case ATTR_DOMAIN_CURVE:
      return "curve";
    case ATTR_DOMAIN_INSTANCE:
      return "instance";
  }

  return "unknown";
}

struct AttributeReadOperator {
  const CDStreamConfig &config;
  const ISampleSelector &sample_sel;
  float velocity_scale;

  const ParsedAttributeDesc *desc;

  bool has_error = false;

  void handle_error(AbcAttributeReadError error,
                    StringRefNull attribute_name,
                    const CacheAttributeMapping *mapping)
  {
    has_error |= error != AbcAttributeReadError::READ_SUCCESS;

    switch (error) {
      case AbcAttributeReadError::READ_SUCCESS:
        return;
      case AbcAttributeReadError::TOO_MANY_UVS_ATTRIBUTES:
        std::cerr << "Cannot read attribute \"" << attribute_name
                  << "\" as a UV map as the limit of UV maps has been reached!\n";
        return;
      case AbcAttributeReadError::TOO_MANY_VCOLS_ATTRIBUTES:
        std::cerr << "Cannot read attribute \"" << attribute_name
                  << "\" as a Vertex Color layer as the limit of Vertex Color layers has been "
                     "reached!\n";
        return;
      case AbcAttributeReadError::INVALID_ATTRIBUTE:
        std::cerr << "Cannot read attribute \"" << attribute_name
                  << "\" as it is invalid! The file or the data may be corrupted\n";
        return;
      case AbcAttributeReadError::MAPPING_IMPOSSIBLE: {
        if (mapping) {
          std::cerr << "Cannot read attribute \"" << attribute_name << "\" as mapping to \""
                    << get_mapping_string(mapping) << "\" on domain " << get_domain_string(mapping)
                    << " is impossible! Try changing the mapping type or the target domain.\n";
        }
        else {
          std::cerr
              << "Cannot read attribute \"" << attribute_name
              << "\" as there is no explicit mapping for it. Try adding an attribute remapping.\n";
        }
        return;
      }
      case AbcAttributeReadError::DOMAIN_RESOLUTION_FAILED:
        std::cerr << "Cannot read attribute \"" << attribute_name
                  << "\" as the domain is ambiguous! Try setting a precise domain.\n";
        return;
      case AbcAttributeReadError::INVALID_DOMAIN:
        std::cerr << "Cannot read attribute \"" << attribute_name
                  << "\" as the domain is invalid for the specific data type!\n";
        return;
      case AbcAttributeReadError::UNSUPPORTED_TYPE:
        std::cerr << "Cannot read attribute \"" << attribute_name << "\""
                  << " as the data type is not supported!\n";
        return;
    }
  }

  void default_handler_for_unsupported_attribute_type(StringRefNull param_name)
  {
    handle_error(AbcAttributeReadError::UNSUPPORTED_TYPE, param_name, nullptr);
  }

  template<typename GeomParamType> void operator()(const GeomParamType &param)
  {
    AbcAttributeReadError error = process_typed_attribute(
        config, desc->mapping, param, sample_sel, velocity_scale);
    handle_error(error, desc->prop_header.getName(), desc->mapping);
  }
};

void read_arbitrary_attributes(const CDStreamConfig &config,
                               const ICompoundProperty &schema,
                               const IV2fGeomParam &primary_uvs,
                               const ISampleSelector &sample_sel,
                               float velocity_scale)
{
  /* Manually extract the arbitrary geometry parameters. We do it this way to avoid complicating
   * the code when dealing with schemas and default velocities which are not accessible via an
   * IGeomParam as we would like for the sake of genericity, but as an IArrayProperty. */
  const PropertyHeader *arb_geom_prop_header = schema.getPropertyHeader(".arbGeomParams");
  ICompoundProperty arb_geom_params;
  if (arb_geom_prop_header) {
    arb_geom_params = ICompoundProperty(schema, ".arbGeomParams");
  }

  BLI_assert_msg(
      config.attribute_helper,
      "read_arbitrary_attributes should not be called without a valid attribute_helper");
  const AttributeReadingHelper &attribute_selector = *config.attribute_helper;

  Vector<ParsedAttributeDesc> attributes = parse_attributes(attribute_selector, arb_geom_params);

  /* At this point the velocities attribute should have the default, standard, attribute velocity
   * name in Alembic. So we also expect any remapping to also use this name. If there is no
   * attribute with the standard name, then either there are no velocities, or it has a different
   * name which should have been set in the UI (both for the selection and the remapping). */
  const PropertyHeader *velocity_prop_header = schema.getPropertyHeader(".velocities");
  if (velocity_prop_header) {
    attributes.append(
        {schema, *velocity_prop_header, attribute_selector.get_mapping(".velocities")});
  }

  if (primary_uvs.valid() && attribute_selector.uvs_requested()) {
    read_mesh_uvs(config, primary_uvs, sample_sel);
  }

  AttributeReadOperator op{config, sample_sel, velocity_scale, nullptr};

  for (const ParsedAttributeDesc &desc : attributes) {
    op.desc = &desc;
    abc_attribute_type_operation(desc.parent, desc.prop_header, op);
  }

  if (op.has_error) {
    const char *attribute_error_message = N_(
        "Errors while trying to read attributes, see console for details...");
    config.attribute_helper->set_attribute_reading_error(attribute_error_message);
  }
}

struct AnimatedAttributeOperator {
  bool default_handler_for_unsupported_attribute_type(const std::string & /*param_name*/)
  {
    return false;
  }

  template<typename ParamType> bool operator()(const ParamType &param)
  {
    return param.valid() && !param.isConstant();
  }
};

bool has_animated_attributes(const ICompoundProperty &arb_geom_params)
{
  /* Since this function is mainly called during data creation when importing an archive, we do not
   * have an attribute helper to pass here. So create a dummy one instead. Also most of its
   * settings (what to read, velocity name, etc.) would not be initialized anyway. */
  AttributeReadingHelper attribute_helper = AttributeReadingHelper::create_default();
  Vector<ParsedAttributeDesc> attributes = parse_attributes(attribute_helper, arb_geom_params);

  AnimatedAttributeOperator animated_param_op;
  for (ParsedAttributeDesc desc : attributes) {
    const PropertyHeader &prop = desc.prop_header;
    if (abc_attribute_type_operation(desc.parent, prop, animated_param_op)) {
      return true;
    }
  }

  return false;
}

void AttributeReadingHelper::set_read_flags(int flags)
{
  read_flags = flags;
}

void AttributeReadingHelper::set_attribute_reading_error_pointer(const char **error_pointer)
{
  attribute_reading_error = error_pointer;
}

void AttributeReadingHelper::set_attribute_reading_error(const char *error_message) const
{
  if (attribute_reading_error) {
    MEM_SAFE_FREE(*attribute_reading_error);
    *attribute_reading_error = BLI_strdup(error_message);
  }
}

const CacheAttributeMapping *AttributeReadingHelper::get_mapping(
    const StringRefNull attr_name) const
{
  for (const CacheAttributeMapping *mapping : mappings_) {
    if (attr_name == mapping->name) {
      return mapping;
    }
  }

  return nullptr;
}

StringRefNull AttributeReadingHelper::velocity_attribute_name() const
{
  return velocity_attribute;
}

AttributeReadingHelper AttributeReadingHelper::create_default()
{
  static ListBase lb = {nullptr, nullptr};
  AttributeReadingHelper attribute_helper(&lb);
  /* If we only load a static frame, or if we are during import before adding a modifier, try to
   * load the velocity with the standard Alembic velocity name. */
  attribute_helper.set_velocity_attribute_name(".velocities");
  /* By default, also load UVs and vertex colors. */
  attribute_helper.set_read_flags(MOD_MESHSEQ_READ_UV | MOD_MESHSEQ_READ_COLOR);
  return attribute_helper;
}

void AttributeReadingHelper::set_velocity_attribute_name(const char *name)
{
  velocity_attribute = name;
}

bool AttributeReadingHelper::uvs_requested() const
{
  return (read_flags & MOD_MESHSEQ_READ_UV) != 0;
}

bool AttributeReadingHelper::vertex_colors_requested() const
{
  return (read_flags & MOD_MESHSEQ_READ_COLOR) != 0;
}

bool AttributeReadingHelper::original_coordinates_requested() const
{
  return (read_flags & MOD_MESHSEQ_READ_VERT) != 0;
}

bool AttributeReadingHelper::should_read_attribute(const StringRefNull attr_name) const
{
  /* Empty names are invalid. Those beginning with a '.' are special and correspond to data
   * accessible through specific APIs (e.g. the vertices are named ".P") */
  if (attr_name.is_empty() || (attr_name[0] == '.' && attr_name != velocity_attribute)) {
    return false;
  }

  return true;
}

}  // namespace blender::io::alembic
