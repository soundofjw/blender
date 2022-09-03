/* SPDX-License-Identifier: GPL-2.0-or-later
 * Copyright 2016 Kévin Dietrich. All rights reserved. */
#pragma once

/** \file
 * \ingroup balembic
 */

#include <Alembic/Abc/All.h>
#include <Alembic/AbcGeom/All.h>

#include <map>

#include "BKE_attribute.h"
#include "BKE_geometry_set.hh"

#include "BLI_listbase_wrapper.hh"
#include "BLI_string_ref.hh"

struct CacheAttributeMapping;
struct CustomData;
struct ID;
struct MLoop;
struct MLoopUV;
struct MPoly;
struct MVert;
struct Mesh;

using Alembic::Abc::ICompoundProperty;
using Alembic::Abc::OCompoundProperty;
namespace blender::io::alembic {

class AttributeReadingHelper;

struct UVSample {
  std::vector<Imath::V2f> uvs;
  std::vector<uint32_t> indices;
};

struct CDStreamConfig {
  MLoop *mloop;
  int totloop;

  MPoly *mpoly;
  int totpoly;

  MVert *mvert;
  int totvert;

  MLoopUV *mloopuv;

  CustomData *loopdata;

  bool pack_uvs;

  /* NOTE: the mesh is mostly used for iterating over loops for loop attributes (UVs, MCol, etc.).
   * It would be nice to remove it, in favor of a more generic way to iterate valid attribute
   * indices.
   */
  Mesh *mesh;

  /* This is used for attribute access. */
  std::unique_ptr<GeometryComponent> geometry_component;

  double weight;
  Alembic::Abc::chrono_t time;
  int timesample_index;
  bool use_vertex_interpolation;
  Alembic::AbcGeom::index_t index;
  Alembic::AbcGeom::index_t ceil_index;

  const char **modifier_error_message;
  const char **attribute_error_message;

  /* For error reporting when reading vertex colors. */
  std::string iobject_full_name;

  /* This is a pointer as using a reference is not possible as the Mesh exporter has it as a
   * member. */
  const AttributeReadingHelper *attribute_helper;

  /* Alembic needs Blender to keep references to C++ objects (the destructors finalize the writing
   * to ABC). The following fields are all used to keep these references. */

  /* Mapping from UV map name to its ABC property, for the 2nd and subsequent UV maps; the primary
   * UV map is kept alive by the Alembic mesh sample itself. */
  std::map<std::string, Alembic::AbcGeom::OV2fGeomParam> abc_uv_maps;

  /* ORCO coordinates, aka Generated Coordinates. */
  Alembic::AbcGeom::OV3fGeomParam abc_orco;

  /* Mapping from vertex color layer name to its Alembic color data. */
  std::map<std::string, Alembic::AbcGeom::OC4fGeomParam> abc_vertex_colors;

  CDStreamConfig()
      : mloop(NULL),
        totloop(0),
        mpoly(NULL),
        totpoly(0),
        totvert(0),
        pack_uvs(false),
        mesh(NULL),
        geometry_component(nullptr),
        weight(0.0),
        time(0.0),
        index(0),
        ceil_index(0),
        modifier_error_message(NULL),
        attribute_helper(nullptr)
  {
  }
};

/* Get the UVs for the main UV property on a OSchema.
 * Returns the name of the UV layer.
 *
 * For now the active layer is used, maybe needs a better way to choose this. */
const char *get_uv_sample(UVSample &sample, const CDStreamConfig &config, CustomData *data);

void write_generated_coordinates(const OCompoundProperty &prop, CDStreamConfig &config);

void write_custom_data(const OCompoundProperty &prop,
                       CDStreamConfig &config,
                       CustomData *data,
                       int data_type);

/* Helper class which holds the data and settings used for attribute loading. */
class AttributeReadingHelper {
  /* Name of the velocity attribute, as the velocity attribute might not use the standard Alembic
   * name or be set using the standard Alembic API (`sample.setVelocities(...)`). */
  StringRefNull velocity_attribute = "";

  /* The flags are the same as #MeshSeqCacheModifierData.read_flag. */
  int read_flags = 0;

  ListBaseWrapper<const CacheAttributeMapping> mappings_;

  /* This is mutable to allow changing only this specific value when reading attributes (in
   * #read_arbitrary_attributes), while all other properties should be immutable at this point.
   * Thread safety is not supposed to be an issue, as a unique instance of this class is used for
   * each modifier. */
  mutable const char **attribute_reading_error = nullptr;

 public:
  explicit AttributeReadingHelper(ListBaseWrapper<const CacheAttributeMapping> mappings)
      : mappings_(mappings)
  {
  }

  /* Create a dummy instance for cases where we need an helper, but we do not have one. This should
   * only happen during import. This also sets default flags and velocity name which are common
   * between the various objects at import time.
   * Using the default constructor (`AttributeReadingHelper(nullptr)` is not really possible as the
   * `ListBaseWrapper` expects a non-null pointer to some `ListBase`. */
  static AttributeReadingHelper create_default();

  void set_velocity_attribute_name(const char *name);
  void set_read_flags(int flags);
  void set_attribute_reading_error_pointer(const char **error_pointer);
  void set_attribute_reading_error(const char *error_message) const;

  const CacheAttributeMapping *get_mapping(const StringRefNull attr_name) const;

  StringRefNull velocity_attribute_name() const;

  /* Return true if `MOD_MESHSEQ_READ_UV` is set in the #read_flags. */
  bool uvs_requested() const;

  /* Return true if `MOD_MESHSEQ_READ_COLOR` is set in the #read_flags. */
  bool vertex_colors_requested() const;

  /* Return true if `MOD_MESHSEQ_READ_VERT` is set in the #read_flags. `MOD_MESHSEQ_READ_VERT` is
   * used as there is no specific flags for original coordinates. */
  bool original_coordinates_requested() const;

  /* Return true if the attribute should be read based on its name. Since Alembic also stores
   * default properties (e.g. positions) as "attributes", we need to filter them out. */
  bool should_read_attribute(const StringRefNull attr_name) const;
};

/* NOTE: default UVs might be stored differently based on the schema, and there does not seem to be
 * a nice generic way to access, so they are passed here as well. */
void read_arbitrary_attributes(const CDStreamConfig &config,
                               const ICompoundProperty &schema,
                               const Alembic::AbcGeom::IV2fGeomParam &primary_uvs,
                               const Alembic::Abc::ISampleSelector &sample_sel,
                               float velocity_scale);

bool has_animated_attributes(const ICompoundProperty &arb_geom_params);

}  // namespace blender::io::alembic
