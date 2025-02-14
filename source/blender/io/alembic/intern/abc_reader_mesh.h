/* SPDX-License-Identifier: GPL-2.0-or-later */
#pragma once

/** \file
 * \ingroup balembic
 */

#include "BLI_span.hh"

#include "abc_customdata.h"
#include "abc_reader_object.h"

struct Mesh;

namespace blender::io::alembic {

class AbcMeshReader final : public AbcObjectReader {
  Alembic::AbcGeom::IPolyMeshSchema m_schema;

 public:
  AbcMeshReader(const Alembic::Abc::IObject &object, ImportSettings &settings);

  bool valid() const override;
  bool accepts_object_type(const Alembic::AbcCoreAbstract::ObjectHeader &alembic_header,
                           const Object *const ob,
                           const char **err_str) const override;
  void readObjectData(Main *bmain, const Alembic::Abc::ISampleSelector &sample_sel) override;

  bool topology_changed(const Mesh *existing_mesh,
                        const Alembic::Abc::ISampleSelector &sample_sel) override;

  void read_geometry(GeometrySet &geometry_set,
                     const Alembic::Abc::ISampleSelector &sample_sel,
                     const AttributeReadingHelper &attribute_helper,
                     int read_flag,
                     const float velocity_scale,
                     const char **err_str) override;

 private:
  void readFaceSetsSample(Main *bmain,
                          Mesh *mesh,
                          const Alembic::AbcGeom::ISampleSelector &sample_sel);

  void assign_facesets_to_mpoly(const Alembic::Abc::ISampleSelector &sample_sel,
                                MPoly *mpoly,
                                int totpoly,
                                std::map<std::string, int> &r_mat_map);

  struct Mesh *read_mesh(struct Mesh *existing_mesh,
                         const Alembic::Abc::ISampleSelector &sample_sel,
                         const AttributeReadingHelper &attribute_helper,
                         const int read_flag,
                         const float velocity_scale,
                         const char **err_str);
  void assign_facesets_to_material_indices(const Alembic::Abc::ISampleSelector &sample_sel,
                                           MutableSpan<int> material_indices,
                                           std::map<std::string, int> &r_mat_map);
};

class AbcSubDReader final : public AbcObjectReader {
  Alembic::AbcGeom::ISubDSchema m_schema;

 public:
  AbcSubDReader(const Alembic::Abc::IObject &object, ImportSettings &settings);

  bool valid() const override;
  bool accepts_object_type(const Alembic::AbcCoreAbstract::ObjectHeader &alembic_header,
                           const Object *const ob,
                           const char **err_str) const override;
  void readObjectData(Main *bmain, const Alembic::Abc::ISampleSelector &sample_sel) override;

  void read_geometry(GeometrySet &geometry_set,
                     const Alembic::Abc::ISampleSelector &sample_sel,
                     const AttributeReadingHelper &attribute_helper,
                     int read_flag,
                     const float velocity_scale,
                     const char **err_str) override;

 private:
  struct Mesh *read_mesh(struct Mesh *existing_mesh,
                         const Alembic::Abc::ISampleSelector &sample_sel,
                         const AttributeReadingHelper &attribute_helper,
                         const int read_flag,
                         const float velocity_scale,
                         const char **err_str);
};

void read_mverts(Mesh &mesh,
                 const Alembic::AbcGeom::P3fArraySamplePtr positions,
                 const Alembic::AbcGeom::N3fArraySamplePtr normals);

void *add_customdata_cb(Mesh *mesh, const char *name, int data_type);

CDStreamConfig get_config(struct Mesh *mesh,
                          const AttributeReadingHelper &attribute_helper,
                          const std::string &iobject_full_name,
                          bool use_vertex_interpolation);

}  // namespace blender::io::alembic
