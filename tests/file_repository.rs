/*
 * Copyright 2019-2020 Wren Powell
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#![cfg(all(feature = "repo-file", feature = "encryption", feature = "compression"))]

use std::collections::HashSet;
use std::fs::{create_dir, File};
use std::io::{Read, Write};
use std::iter::FromIterator;

#[cfg(all(target_os = "linux", feature = "file-metadata"))]
use exacl::{AclEntry, AclEntryKind, AclOption, Flag, Perm};
use relative_path::RelativePathBuf;
use tempfile::TempDir;

use acid_store::repo::file::{Entry, FileMode, FileRepo, WalkPredicate};
use acid_store::repo::{Commit, SwitchInstance, DEFAULT_INSTANCE};

use acid_store::uuid::Uuid;
use common::*;
#[cfg(all(unix, feature = "file-metadata"))]
use {
    acid_store::repo::file::{
        Acl, AclMode, AclQualifier, CommonMetadata, EntryType, NoMetadata, NoSpecial, UnixMetadata,
        UnixSpecial,
    },
    maplit::hashmap,
    nix::sys::stat::{Mode, SFlag},
    nix::unistd::mkfifo,
    std::collections::HashMap,
    std::fs::read_link,
    std::os::unix::fs::{symlink, MetadataExt},
    std::path::Path,
    std::time::{Duration, SystemTime},
};

mod common;

#[rstest]
fn switching_instance_does_not_roll_back(
    mut repo: FileRepo,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    repo.create("file", &Entry::file())?;
    let mut object = repo.open("file")?;
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    let repo: FileRepo = repo.switch_instance(Uuid::new_v4().into())?;
    let repo: FileRepo = repo.switch_instance(DEFAULT_INSTANCE)?;

    assert_that!(repo.exists("file")).is_true();
    assert_that!(repo.open("file")).is_ok();

    Ok(())
}

#[rstest]
fn switching_instance_does_not_commit(mut repo: FileRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    repo.create("file", &Entry::file())?;
    let mut object = repo.open("file")?;
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    let repo: FileRepo = repo.switch_instance(Uuid::new_v4().into())?;
    let mut repo: FileRepo = repo.switch_instance(DEFAULT_INSTANCE)?;
    repo.rollback()?;

    assert_that!(repo.exists("file")).is_false();
    assert_that!(repo.open("file")).is_err_variant(acid_store::Error::NotFound);

    Ok(())
}

#[rstest]
fn empty_path_does_not_exist(repo: FileRepo) {
    assert_that!(repo.exists("")).is_false();
    assert_that!(repo.is_file("")).is_false();
    assert_that!(repo.is_directory("")).is_false();
    assert_that!(repo.is_special("")).is_false();
}

#[rstest]
fn nonexistent_child_does_not_exist(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("parent", &Entry::directory())?;

    assert_that!(repo.exists("parent/nonexistent")).is_false();
    assert_that!(repo.is_file("parent/nonexistent")).is_false();
    assert_that!(repo.is_directory("parent/nonexistent")).is_false();
    assert_that!(repo.is_special("parent/nonexistent")).is_false();

    Ok(())
}

#[rstest]
fn check_entry_type(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("file", &Entry::file())?;
    repo.create("directory", &Entry::directory())?;

    assert_that!(repo.exists("file")).is_true();
    assert_that!(repo.exists("directory")).is_true();

    assert_that!(repo.is_file("file")).is_true();
    assert_that!(repo.is_directory("file")).is_false();
    assert_that!(repo.is_special("file")).is_false();

    assert_that!(repo.is_file("directory")).is_false();
    assert_that!(repo.is_directory("directory")).is_true();
    assert_that!(repo.is_special("directory")).is_false();

    Ok(())
}

#[rstest]
fn creating_existing_file_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("home", &Entry::directory())?;
    assert_that!(repo.create("home", &Entry::directory()))
        .is_err_variant(acid_store::Error::AlreadyExists);
    Ok(())
}

#[rstest]
fn creating_file_without_parent_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    // Creating a directory without a parent fails.
    assert_that!(repo.create("nonexistent/child", &Entry::directory()))
        .is_err_variant(acid_store::Error::NotFound);

    // Creating a directory as a child of a file fails.
    repo.create("file", &Entry::file())?;
    assert_that!(repo.create("file/child", &Entry::directory()))
        .is_err_variant(acid_store::Error::NotDirectory);

    Ok(())
}

#[rstest]
fn creating_empty_path_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    assert_that!(repo.create("", &Entry::file())).is_err_variant(acid_store::Error::InvalidPath);
    assert_that!(repo.create_parents("", &Entry::file()))
        .is_err_variant(acid_store::Error::InvalidPath);

    Ok(())
}

#[rstest]
fn create_parents(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("home/lostatc/test", &Entry::file())?;

    assert_that!(repo.is_file("home/lostatc/test")).is_true();
    assert_that!(repo.is_directory("home/lostatc")).is_true();
    assert_that!(repo.is_directory("home")).is_true();

    assert_that!(repo.entry("home/lostatc/test")?.is_file()).is_true();
    assert_that!(repo.entry("home/lostatc")?.is_directory()).is_true();
    assert_that!(repo.entry("home")?.is_directory()).is_true();

    Ok(())
}

#[rstest]
fn create_parent_of_top_level_file(mut repo: FileRepo) -> anyhow::Result<()> {
    assert_that!(repo.create_parents("home", &Entry::directory())).is_ok();
    assert_that!(repo.is_directory("home")).is_true();
    assert_that!(repo.entry("home")?.is_directory()).is_true();

    Ok(())
}

#[rstest]
fn removing_nonexistent_path_errs(mut repo: FileRepo) {
    assert_that!(repo.remove("nonexistent")).is_err_variant(acid_store::Error::NotFound);
    assert_that!(repo.remove_tree("nonexistent")).is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn removing_non_empty_directory_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("parent/directory", &Entry::directory())?;
    assert_that!(repo.remove("parent")).is_err_variant(acid_store::Error::NotEmpty);
    Ok(())
}

#[rstest]
fn removing_emtpy_path_errs(mut repo: FileRepo) {
    assert_that!(repo.remove("")).is_err_variant(acid_store::Error::InvalidPath);
    assert_that!(repo.remove_tree("")).is_err_variant(acid_store::Error::InvalidPath);
}

#[rstest]
fn remove_file(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("test", &Entry::directory())?;
    repo.remove("test")?;

    assert_that!(repo.exists("test")).is_false();

    Ok(())
}

#[rstest]
fn remove_tree(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("home/lostatc/test", &Entry::file())?;
    repo.remove_tree("home")?;

    assert_that!(repo.exists("home")).is_false();
    assert_that!(repo.exists("home/lostatc")).is_false();
    assert_that!(repo.exists("home/lostatc/test")).is_false();

    Ok(())
}

#[rstest]
fn remove_tree_without_descendants(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("home", &Entry::directory())?;
    repo.remove_tree("home")?;

    assert_that!(repo.exists("home")).is_false();

    Ok(())
}

#[rstest]
fn getting_entry_of_empty_path_errs(repo: FileRepo) {
    assert_that!(repo.entry("")).is_err_variant(acid_store::Error::InvalidPath);
}

#[rstest]
fn getting_nonexistent_entry_errs(repo: FileRepo) {
    assert_that!(repo.entry("nonexistent")).is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn getting_entry_id_of_empty_path_errs(repo: FileRepo) {
    assert_that!(repo.entry_id("")).is_err_variant(acid_store::Error::InvalidPath);
}

#[rstest]
fn getting_nonexistent_entry_id_errs(repo: FileRepo) {
    assert_that!(repo.entry_id("nonexistent")).is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn setting_metadata_on_nonexistent_file_errs(mut repo: FileRepo) {
    assert_that!(repo.set_metadata("file", None)).is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn setting_metadata_on_empty_path_errs(mut repo: FileRepo) {
    assert_that!(repo.set_metadata("", None)).is_err_variant(acid_store::Error::InvalidPath);
}

#[rstest]
#[cfg(feature = "file-metadata")]
fn set_common_metadata(mut repo: FileRepo<NoSpecial, CommonMetadata>) -> anyhow::Result<()> {
    let expected_metadata = CommonMetadata {
        modified: SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        accessed: SystemTime::UNIX_EPOCH + Duration::from_secs(20),
    };
    repo.create("file", &Entry::file())?;
    repo.set_metadata("file", Some(expected_metadata.clone()))?;
    let actual_metadata = repo.entry("file")?.metadata;

    assert_that!(actual_metadata).contains_value(expected_metadata);

    Ok(())
}

#[rstest]
fn open_file(mut repo: FileRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    repo.create("file", &Entry::file())?;
    let mut object = repo.open("file")?;

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    let mut object = repo.open("file")?;
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;

    assert_that!(actual_data).is_equal_to(&buffer);

    Ok(())
}

#[rstest]
fn opening_empty_path_errs(repo: FileRepo) {
    assert_that!(repo.open("")).is_err_variant(acid_store::Error::InvalidPath);
}

#[rstest]
fn opening_nonexistent_entry_errs(repo: FileRepo) {
    assert_that!(repo.open("nonexistent")).is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn opening_directory_entry_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("directory", &Entry::directory())?;

    assert_that!(repo.open("directory")).is_err_variant(acid_store::Error::NotFile);

    Ok(())
}

#[rstest]
fn copied_file_has_same_contents(mut repo: FileRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    // Add a file entry and write data to it.
    repo.create("source", &Entry::file())?;
    let mut object = repo.open("source")?;
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    // Copy the file entry.
    repo.copy("source", "dest")?;

    let mut actual_data = Vec::new();
    let mut object = repo.open("dest")?;
    object.read_to_end(&mut actual_data)?;

    assert_that!(actual_data).is_equal_to(&buffer);

    Ok(())
}

#[rstest]
fn copied_entry_has_different_entry_id(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;
    let source_id = repo.entry_id("source")?;

    repo.copy("source", "dest")?;

    assert_that!(repo.entry_id("dest"))
        .is_ok()
        .is_not_equal_to(source_id);

    Ok(())
}

#[rstest]
fn copying_nonexistent_source_errs(mut repo: FileRepo) {
    assert_that!(repo.copy("nonexistent", "dest")).is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn copy_entry_with_invalid_destination_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;

    assert_that!(repo.copy("source", "nonexistent/dest"))
        .is_err_variant(acid_store::Error::NotFound);

    repo.create("file", &Entry::file())?;

    assert_that!(repo.copy("source", "file/dest")).is_err_variant(acid_store::Error::NotDirectory);

    repo.create("dest", &Entry::directory())?;

    assert_that!(repo.copy("source", "dest")).is_err_variant(acid_store::Error::AlreadyExists);

    Ok(())
}

#[rstest]
fn copy_tree(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("source/file1", &Entry::file())?;
    repo.create_parents("source/directory/file2", &Entry::file())?;

    repo.copy_tree("source", "dest")?;

    assert_that!(repo.is_file("dest/file1")).is_true();
    assert_that!(repo.is_file("dest/directory/file2")).is_true();
    assert_that!(repo.is_directory("dest/directory")).is_true();

    assert_that!(repo.entry("dest/file1")?.is_file()).is_true();
    assert_that!(repo.entry("dest/directory/file2")?.is_file()).is_true();
    assert_that!(repo.entry("dest/directory")?.is_directory()).is_true();

    Ok(())
}

#[rstest]
fn copy_subdirectory_tree(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("root/source/file1", &Entry::file())?;

    repo.copy_tree("root/source", "root/dest")?;

    assert_that!(repo.is_file("root/dest/file1")).is_true();
    assert_that!(repo.entry("root/dest/file1")?.is_file()).is_true();

    Ok(())
}

#[rstest]
fn copy_tree_which_is_a_file(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;

    repo.copy_tree("source", "dest")?;

    assert_that!(repo.is_file("dest")).is_true();
    assert_that!(repo.entry("dest")?.is_file()).is_true();

    Ok(())
}

#[rstest]
fn copying_with_empty_path_as_source_errs(mut repo: FileRepo) {
    assert_that!(repo.copy("", "test")).is_err_variant(acid_store::Error::InvalidPath);
    assert_that!(repo.copy_tree("", "test")).is_err_variant(acid_store::Error::InvalidPath);
}

#[rstest]
fn copying_with_empty_path_as_dest_errs(mut repo: FileRepo) {
    assert_that!(repo.copy("test", "")).is_err_variant(acid_store::Error::InvalidPath);
    assert_that!(repo.copy_tree("test", "")).is_err_variant(acid_store::Error::InvalidPath);
}

#[rstest]
fn renaming_nonexistent_source_errs(mut repo: FileRepo) {
    assert_that!(repo.rename("nonexistent", "dest")).is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn rename_entry_with_invalid_destination_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;

    assert_that!(repo.rename("source", "nonexistent/dest"))
        .is_err_variant(acid_store::Error::NotFound);

    repo.create("file", &Entry::file())?;

    assert_that!(repo.rename("source", "file/dest"))
        .is_err_variant(acid_store::Error::NotDirectory);

    repo.create("dest", &Entry::directory())?;

    assert_that!(repo.rename("source", "dest")).is_err_variant(acid_store::Error::AlreadyExists);

    Ok(())
}

#[rstest]
fn rename_with_empty_source_errs(mut repo: FileRepo) {
    assert_that!(repo.rename("", "dest")).is_err_variant(acid_store::Error::InvalidPath);
}

#[rstest]
fn rename_with_empty_dest_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;

    assert_that!(repo.rename("source", "")).is_err_variant(acid_store::Error::InvalidPath);

    Ok(())
}

#[rstest]
fn rename_removes_source_tree(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("source/file1", &Entry::file())?;
    repo.create_parents("source/directory/file2", &Entry::file())?;

    assert_that!(repo.rename("source", "dest")).is_ok();
    assert_that!(repo.exists("source")).is_false();

    Ok(())
}

#[rstest]
fn rename_subdirectory_tree(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("root/source/file1", &Entry::file())?;

    assert_that!(repo.rename("root/source", "root/dest")).is_ok();

    assert_that!(repo.is_file("root/dest/file1")).is_true();
    assert_that!(repo.entry("root/dest/file1")?.is_file()).is_true();

    Ok(())
}

#[rstest]
fn rename_tree_preserves_links(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("source/file1", &Entry::file())?;
    repo.create_parents("source/directory/file2", &Entry::file())?;
    repo.link("source/file1", "link")?;
    let link_id = repo.entry_id("link")?;

    assert_that!(repo.rename("source", "dest")).is_ok();
    assert_that!(repo.entry_id("dest/file1")).is_ok_containing(link_id);
    assert_that!(repo.link_count(link_id)).is_equal_to(2);

    Ok(())
}

#[rstest]
fn rename_file_preserves_links(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;
    repo.link("source", "link")?;
    let link_id = repo.entry_id("link")?;

    assert_that!(repo.rename("source", "dest")).is_ok();
    assert_that!(repo.entry_id("dest")).is_ok_containing(link_id);
    assert_that!(repo.link_count(link_id)).is_equal_to(2);

    Ok(())
}

#[rstest]
fn rename_tree_preserves_entry_ids(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("source/file1", &Entry::file())?;
    repo.create_parents("source/directory/file2", &Entry::file())?;
    let file1_id = repo.entry_id("source/file1")?;
    let file2_id = repo.entry_id("source/directory/file2")?;

    assert_that!(repo.rename("source", "dest")).is_ok();
    assert_that!(repo.entry_id("dest/file1")).is_ok_containing(file1_id);
    assert_that!(repo.entry_id("dest/directory/file2")).is_ok_containing(file2_id);

    Ok(())
}

#[rstest]
fn rename_file_preserves_entry_id(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;
    let entry_id = repo.entry_id("source")?;

    assert_that!(repo.rename("source", "dest")).is_ok();
    assert_that!(repo.entry_id("dest")).is_ok_containing(entry_id);

    Ok(())
}

#[rstest]
fn renaming_tree_to_subdirectory_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::directory())?;

    assert_that!(repo.rename("source", "source/descendant"))
        .is_err_variant(acid_store::Error::InvalidPath);

    Ok(())
}

#[rstest]
fn linking_directory_entry_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::directory())?;
    assert_that!(repo.link("source", "dest")).is_err_variant(acid_store::Error::NotFile);
    Ok(())
}

#[rstest]
fn linking_with_empty_path_as_source_errs(mut repo: FileRepo) {
    assert_that!(repo.link("", "test")).is_err_variant(acid_store::Error::InvalidPath);
}

#[rstest]
fn linking_with_empty_path_as_dest_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("test", &Entry::file())?;
    assert_that!(repo.link("test", "")).is_err_variant(acid_store::Error::InvalidPath);
    Ok(())
}

#[rstest]
fn linking_to_existing_dest_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;
    repo.create("dest", &Entry::file())?;
    assert_that!(repo.link("source", "dest")).is_err_variant(acid_store::Error::AlreadyExists);
    Ok(())
}

#[rstest]
fn linking_to_dest_with_nonexistent_parent_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;

    assert_that!(repo.link("source", "nonexistent/dest"))
        .is_err_variant(acid_store::Error::NotFound);

    Ok(())
}

#[rstest]
fn linking_to_dest_with_file_parent_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;
    repo.create("parent", &Entry::file())?;
    assert_that!(repo.link("source", "parent/dest"))
        .is_err_variant(acid_store::Error::NotDirectory);
    Ok(())
}

#[rstest]
fn linking_with_nonexistent_source_errs(mut repo: FileRepo) {
    assert_that!(repo.link("source", "dest")).is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
#[cfg(all(unix, feature = "file-metadata"))]
fn linked_entries_share_metadata(
    mut repo: FileRepo<NoSpecial, CommonMetadata>,
) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;
    repo.link("source", "dest")?;

    let expected_metadata = CommonMetadata {
        modified: SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        accessed: SystemTime::UNIX_EPOCH + Duration::from_secs(20),
    };

    repo.set_metadata("source", Some(expected_metadata.clone()))?;

    let actual_metadata = repo.entry("dest")?.metadata;

    assert_that!(actual_metadata).contains(&expected_metadata);

    Ok(())
}

#[rstest]
fn linked_entries_share_contents(mut repo: FileRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    repo.create("source", &Entry::file())?;
    repo.link("source", "dest")?;

    let mut object = repo.open("source")?;
    object.write_all(&buffer)?;
    object.commit()?;

    let mut contents = Vec::new();
    let mut object = repo.open("dest")?;
    object.read_to_end(&mut contents)?;

    assert_that!(contents).is_equal_to(&buffer);

    Ok(())
}

#[rstest]
fn removing_entry_does_not_affect_linked_entry(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("one", &Entry::file())?;
    repo.link("one", "two")?;
    repo.remove("one")?;

    assert_that!(repo.entry("two")).is_ok();
    assert_that!(repo.open("two")).is_ok();

    repo.remove("two")?;

    assert_that!(repo.entry("two")).is_err_variant(acid_store::Error::NotFound);
    assert_that!(repo.open("two")).is_err_variant(acid_store::Error::NotFound);

    Ok(())
}

#[rstest]
fn link_count_returns_number_of_links(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("one", &Entry::file())?;
    let entry_id = repo.entry_id("one")?;

    assert_that!(repo.link_count(entry_id)).is_equal_to(1);

    repo.link("one", "two")?;

    assert_that!(repo.link_count(entry_id)).is_equal_to(2);

    repo.link("one", "three")?;

    assert_that!(repo.link_count(entry_id)).is_equal_to(3);

    repo.remove("two")?;

    assert_that!(repo.link_count(entry_id)).is_equal_to(2);

    repo.remove("one")?;
    repo.remove("three")?;

    assert_that!(repo.link_count(entry_id)).is_equal_to(0);

    Ok(())
}

#[rstest]
fn list_children(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("root/child1", &Entry::file())?;
    repo.create_parents("root/child2/descendant", &Entry::file())?;

    assert_that!(repo.children("root")?.collect::<Vec<_>>()).contains_all_of(&[
        &RelativePathBuf::from("root/child1"),
        &RelativePathBuf::from("root/child2"),
    ]);

    Ok(())
}

#[rstest]
fn listing_children_of_nonexistent_directory_errs(repo: FileRepo) {
    assert_that!(repo.children("nonexistent").map(Vec::from_iter))
        .is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn listing_children_of_a_file_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("file", &Entry::file())?;
    assert_that!(repo.children("file").map(Vec::from_iter))
        .is_err_variant(acid_store::Error::NotDirectory);
    Ok(())
}

#[rstest]
fn list_children_of_empty_path(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("file1", &Entry::file())?;
    repo.create("file2", &Entry::file())?;

    assert_that!(repo.children("")?.collect::<Vec<_>>()).contains_all_of(&[
        &RelativePathBuf::from("file1"),
        &RelativePathBuf::from("file2"),
    ]);

    Ok(())
}

#[rstest]
fn list_descendants(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("root/child1", &Entry::file())?;
    repo.create_parents("root/child2/descendant", &Entry::file())?;

    assert_that!(repo.descendants("root")?.collect::<Vec<_>>()).contains_all_of(&[
        &RelativePathBuf::from("root/child1"),
        &RelativePathBuf::from("root/child2"),
        &RelativePathBuf::from("root/child2/descendant"),
    ]);

    Ok(())
}

#[rstest]
fn listing_descendants_of_nonexistent_directory_errs(repo: FileRepo) {
    assert_that!(repo.descendants("nonexistent").map(Vec::from_iter))
        .is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn listing_descendants_of_a_file_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("file", &Entry::file())?;

    assert_that!(repo.descendants("file").map(Vec::from_iter))
        .is_err_variant(acid_store::Error::NotDirectory);

    Ok(())
}

#[rstest]
fn list_descendants_of_empty_path(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("directory/file", &Entry::file())?;

    assert_that!(repo.descendants("")?.collect::<Vec<_>>()).contains_all_of(&[
        &RelativePathBuf::from("directory"),
        &RelativePathBuf::from("directory/file"),
    ]);

    Ok(())
}

#[rstest]
fn walking_nonexistent_entry_errs(repo: FileRepo) {
    assert_that!(repo.walk::<(), _, _>("nonexistent", |_| WalkPredicate::Continue))
        .is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn walking_file_entry_errs(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("file", &Entry::file())?;

    assert_that!(repo.walk::<(), _, _>("file", |_| WalkPredicate::Continue))
        .is_err_variant(acid_store::Error::NotDirectory);

    Ok(())
}

#[rstest]
fn archiving_to_dest_with_nonexistent_parent_errs(
    mut repo: FileRepo,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");
    File::create(&source_path)?;

    assert_that!(repo.archive(&source_path, "nonexistent/dest"))
        .is_err_variant(acid_store::Error::NotFound);
    assert_that!(repo.archive_tree(&source_path, "nonexistent/dest"))
        .is_err_variant(acid_store::Error::NotFound);

    Ok(())
}

#[rstest]
fn archiving_nonexistent_source_errs(mut repo: FileRepo, temp_dir: TempDir) {
    let source_path = temp_dir.as_ref().join("nonexistent");

    assert_that!(repo.archive(&source_path, "dest")).is_err_variant(acid_store::Error::NotFound);
    assert_that!(repo.archive_tree(&source_path, "dest"))
        .is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn archiving_to_dest_with_file_parent_errs(
    mut repo: FileRepo,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");
    File::create(&source_path)?;

    repo.create("file", &Entry::file())?;

    assert_that!(repo.archive(&source_path, "file/dest"))
        .is_err_variant(acid_store::Error::NotDirectory);
    assert_that!(repo.archive_tree(&source_path, "file/dest"))
        .is_err_variant(acid_store::Error::NotDirectory);

    Ok(())
}

#[rstest]
fn archiving_with_existing_dest_errs(mut repo: FileRepo, temp_dir: TempDir) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");
    File::create(&source_path)?;

    repo.create("dest", &Entry::directory())?;

    assert_that!(repo.archive(&source_path, "dest"))
        .is_err_variant(acid_store::Error::AlreadyExists);
    assert_that!(repo.archive_tree(&source_path, "dest"))
        .is_err_variant(acid_store::Error::AlreadyExists);

    Ok(())
}

#[rstest]
fn archiving_to_empty_path_errs(mut repo: FileRepo, temp_dir: TempDir) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");
    File::create(&source_path)?;

    assert_that!(repo.archive(&source_path, "")).is_err_variant(acid_store::Error::InvalidPath);
    assert_that!(repo.archive_tree(&source_path, ""))
        .is_err_variant(acid_store::Error::InvalidPath);

    Ok(())
}

#[rstest]
#[cfg(unix)]
fn archiving_unsupported_file_type_errs(
    mut repo: FileRepo,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");
    mkfifo(&source_path, Mode::S_IRWXU)?;

    assert_that!(repo.archive(&source_path, "dest")).is_err_variant(acid_store::Error::FileType);

    Ok(())
}

#[rstest]
fn archive_file(mut repo: FileRepo, temp_dir: TempDir, buffer: Vec<u8>) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");
    let mut source_file = File::create(&source_path)?;
    source_file.write_all(&buffer)?;
    source_file.flush()?;

    repo.archive(&source_path, "dest")?;

    let mut object = repo.open("dest")?;
    let mut actual_contents = Vec::new();
    object.read_to_end(&mut actual_contents)?;

    assert_that!(actual_contents).is_equal_to(buffer);

    Ok(())
}

#[rstest]
#[cfg(all(unix, feature = "file-metadata"))]
fn archive_unix_special_files(
    mut repo: FileRepo<UnixSpecial, NoMetadata>,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let fifo_path = temp_dir.as_ref().join("fifo");
    let symlink_path = temp_dir.as_ref().join("symlink");
    let device_path = Path::new("/dev/null");

    mkfifo(&fifo_path, Mode::S_IRWXU)?;
    symlink("/dev/null", &symlink_path)?;

    repo.create("dest", &Entry::directory())?;
    repo.archive(fifo_path, "dest/fifo")?;
    repo.archive(symlink_path, "dest/symlink")?;
    repo.archive(device_path, "dest/device")?;

    let fifo_entry = repo.entry("dest/fifo")?;
    let symlink_entry = repo.entry("dest/symlink")?;
    let device_entry = repo.entry("dest/device")?;

    assert_that!(fifo_entry.kind).is_equal_to(EntryType::Special(UnixSpecial::NamedPipe));
    assert_that!(symlink_entry.kind).is_equal_to(EntryType::Special(UnixSpecial::Symlink {
        target: "/dev/null".into(),
    }));
    assert_that!(device_entry.kind).is_equal_to(EntryType::Special(UnixSpecial::CharDevice {
        major: 1,
        minor: 3,
    }));

    Ok(())
}

#[rstest]
fn archive_tree(mut repo: FileRepo, temp_dir: TempDir) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");

    create_dir(&source_path)?;
    File::create(&source_path.join("file1"))?;
    create_dir(&source_path.join("directory"))?;
    File::create(&source_path.join("directory/file2"))?;

    repo.archive_tree(&source_path, "dest")?;

    assert_that!(repo.is_directory("dest")).is_true();
    assert_that!(repo.is_file("dest/file1")).is_true();
    assert_that!(repo.is_directory("dest/directory")).is_true();
    assert_that!(repo.is_file("dest/directory/file2")).is_true();

    assert_that!(repo.entry("dest")?.is_directory()).is_true();
    assert_that!(repo.entry("dest/file1")?.is_file()).is_true();
    assert_that!(repo.entry("dest/directory")?.is_directory()).is_true();
    assert_that!(repo.entry("dest/directory/file2")?.is_file()).is_true();

    Ok(())
}

#[rstest]
#[cfg(unix)]
fn archive_tree_skips_unsupported_file_types(
    mut repo: FileRepo,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");
    let special_file = source_path.join("special");
    create_dir(&source_path)?;
    mkfifo(&special_file, Mode::S_IRWXU)?;

    assert_that!(repo.archive_tree(&source_path, "dest")).is_ok();

    assert_that!(repo.exists("dest/special")).is_false();
    assert_that!(repo.entry("dest/special")).is_err_variant(acid_store::Error::NotFound);
    assert_that!(repo
        .children("dest")
        .map(|children| children.collect::<Vec<_>>()))
    .is_ok()
    .is_empty();

    Ok(())
}

#[rstest]
fn extracting_from_empty_path_errs(repo: FileRepo, temp_dir: TempDir) -> anyhow::Result<()> {
    let dest_path = temp_dir.as_ref().join("dest");

    assert_that!(repo.extract("", &dest_path)).is_err_variant(acid_store::Error::InvalidPath);
    assert_that!(repo.extract_tree("", &dest_path)).is_err_variant(acid_store::Error::InvalidPath);

    Ok(())
}

#[rstest]
fn extracting_nonexisting_source_entry_errs(repo: FileRepo, temp_dir: TempDir) {
    let dest_path = temp_dir.as_ref().join("dest");
    assert_that!(repo.extract("nonexistent", &dest_path))
        .is_err_variant(acid_store::Error::NotFound);
    assert_that!(repo.extract_tree("nonexistent", &dest_path))
        .is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn extracting_file_with_existing_dest_errs(
    mut repo: FileRepo,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let dest_path = temp_dir.as_ref().join("dest");
    File::create(&dest_path)?;

    repo.create("source", &Entry::file())?;

    assert_that!(repo.extract("source", &dest_path))
        .is_err_variant(acid_store::Error::AlreadyExists);
    assert_that!(repo.extract_tree("source", &dest_path))
        .is_err_variant(acid_store::Error::AlreadyExists);

    Ok(())
}

#[rstest]
fn extract_file(mut repo: FileRepo, buffer: Vec<u8>, temp_dir: TempDir) -> anyhow::Result<()> {
    let dest_path = temp_dir.as_ref().join("dest");

    repo.create("source", &Entry::file())?;
    let mut object = repo.open("source")?;
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);
    repo.extract("source", &dest_path)?;

    let mut actual_contents = Vec::new();
    let mut dest_file = File::open(&dest_path)?;
    dest_file.read_to_end(&mut actual_contents)?;

    assert_that!(actual_contents).is_equal_to(&buffer);

    Ok(())
}

#[rstest]
#[cfg(all(unix, feature = "file-metadata"))]
fn extract_unix_special_files(
    mut repo: FileRepo<UnixSpecial, NoMetadata>,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let fifo_path = temp_dir.as_ref().join("fifo");
    let symlink_path = temp_dir.as_ref().join("symlink");
    let device_path = temp_dir.as_ref().join("device");

    repo.create("fifo", &Entry::special(UnixSpecial::NamedPipe))?;
    repo.create(
        "symlink",
        &Entry::special(UnixSpecial::Symlink {
            target: "/dev/null".into(),
        }),
    )?;
    repo.create(
        "device",
        &Entry::special(UnixSpecial::CharDevice { major: 1, minor: 3 }),
    )?;

    // The device won't be extracted unless the user has sufficient permissions. In this case, the
    // operation is supposed to silently fail. Assuming the tests are being run without root
    // permissions, we attempt to extract the device to ensure it doesn't return an error, but we
    // don't check to see if it was created.
    repo.extract("fifo", &fifo_path)?;
    repo.extract("symlink", &symlink_path)?;
    repo.extract("device", &device_path)?;

    assert_that!(SFlag::from_bits(
        fifo_path.metadata()?.mode() & SFlag::S_IFMT.bits()
    ))
    .contains_value(SFlag::S_IFIFO);
    assert_that!(read_link(&symlink_path)?).is_equal_to(Path::new("/dev/null").to_path_buf());

    Ok(())
}

#[rstest]
fn extract_tree(mut repo: FileRepo, temp_dir: TempDir) -> anyhow::Result<()> {
    let dest_path = temp_dir.as_ref().join("dest");

    repo.create("source", &Entry::directory())?;
    repo.create("source/file1", &Entry::file())?;
    repo.create("source/directory", &Entry::directory())?;
    repo.create("source/directory/file2", &Entry::file())?;

    repo.extract_tree("source", &dest_path)?;

    assert_that!(dest_path.join("file1")).is_a_file();
    assert_that!(dest_path.join("directory")).is_a_directory();
    assert_that!(dest_path.join("directory/file2")).is_a_file();

    Ok(())
}

#[rstest]
#[cfg(all(unix, feature = "file-metadata"))]
fn write_unix_metadata(
    mut repo: FileRepo<NoSpecial, UnixMetadata>,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let dest_path = temp_dir.as_ref().join("dest");

    // This does not test extended attributes because user extended attributes are not supported
    // on tmpfs, which is most likely where the temporary directory will be created.

    let entry_metadata = UnixMetadata {
        mode: FileMode::S_IWUSR | FileMode::S_IRGRP | FileMode::S_IROTH | FileMode::S_IWOTH,
        modified: SystemTime::UNIX_EPOCH + Duration::from_secs(10),
        accessed: SystemTime::UNIX_EPOCH + Duration::from_secs(20),
        changed: SystemTime::UNIX_EPOCH + Duration::from_secs(30),
        user: 1000,
        group: 1000,
        attributes: HashMap::new(),
        acl: Acl {
            access: hashmap! { AclQualifier::User(65533) => AclMode::R },
            default: HashMap::new(),
        },
    };
    let entry = Entry {
        kind: EntryType::File,
        metadata: Some(entry_metadata.clone()),
    };

    repo.create("source", &entry)?;
    repo.extract("source", &dest_path)?;

    let dest_metadata = dest_path.metadata()?;

    assert_that!(FileMode::from_bits_truncate(dest_metadata.mode() & 0o777))
        .is_equal_to(entry_metadata.mode);
    assert_that!(dest_metadata.modified()).is_ok_containing(entry_metadata.modified);
    assert_that!(dest_metadata.accessed()).is_ok_containing(entry_metadata.accessed);

    #[cfg(target_os = "linux")]
    {
        let actual_entries = exacl::getfacl(dest_path, AclOption::ACCESS_ACL)?;
        let user_entry = AclEntry {
            kind: AclEntryKind::User,
            name: String::new(),
            perms: Perm::WRITE,
            flags: Flag::empty(),
            allow: true,
        };
        let group_entry = AclEntry {
            kind: AclEntryKind::Group,
            name: String::new(),
            perms: Perm::READ,
            flags: Flag::empty(),
            allow: true,
        };
        let other_entry = AclEntry {
            kind: AclEntryKind::Other,
            name: String::new(),
            perms: Perm::READ | Perm::WRITE,
            flags: Flag::empty(),
            allow: true,
        };
        let mask_entry = AclEntry {
            kind: AclEntryKind::Mask,
            name: String::new(),
            perms: Perm::READ,
            flags: Flag::empty(),
            allow: true,
        };
        let new_entry = AclEntry {
            kind: AclEntryKind::User,
            name: "65533".to_string(),
            perms: Perm::READ,
            flags: Flag::empty(),
            allow: true,
        };

        assert_that!(actual_entries).has_length(5);
        assert_that!(actual_entries).contains_all_of(&[
            &user_entry,
            &group_entry,
            &other_entry,
            &mask_entry,
            &new_entry,
        ]);
    }

    Ok(())
}

#[rstest]
#[cfg(all(unix, feature = "file-metadata"))]
fn read_unix_metadata(
    mut repo: FileRepo<NoSpecial, UnixMetadata>,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");
    File::create(&source_path)?;
    let source_metadata = source_path.metadata()?;

    #[cfg(target_os = "linux")]
    {
        let mut entries = exacl::getfacl(&source_path, AclOption::ACCESS_ACL)?;
        entries.push(AclEntry {
            kind: AclEntryKind::User,
            name: "65533".to_string(),
            perms: Perm::READ | Perm::WRITE | Perm::EXECUTE,
            flags: Flag::empty(),
            allow: true,
        });
        exacl::setfacl(&[&source_path], &entries, AclOption::ACCESS_ACL)?;
    }

    repo.archive(&source_path, "dest")?;
    let entry = repo.entry("dest")?;
    let entry_metadata = entry.metadata.unwrap();

    // This does not test extended attributes because user extended attributes are not supported
    // on tmpfs, which is most likely where the temporary directory will be created.

    assert_that!(entry_metadata.mode)
        .is_equal_to(FileMode::from_bits_truncate(source_metadata.mode()));
    assert_that!(entry_metadata.modified).is_equal_to(source_metadata.modified()?);
    assert_that!(entry_metadata.user).is_equal_to(source_metadata.uid());
    assert_that!(entry_metadata.group).is_equal_to(source_metadata.gid());

    #[cfg(target_os = "linux")]
    {
        assert_that!(entry_metadata
            .acl
            .access
            .get(&AclQualifier::User(65533))
            .copied())
        .contains_value(AclMode::RWX);
    }

    Ok(())
}

#[rstest]
#[cfg(all(unix, feature = "file-metadata"))]
fn write_common_metadata(
    mut repo: FileRepo<NoSpecial, CommonMetadata>,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let dest_path = temp_dir.as_ref().join("dest");

    let entry_metadata = CommonMetadata {
        modified: SystemTime::UNIX_EPOCH,
        accessed: SystemTime::UNIX_EPOCH,
    };
    let entry = Entry {
        kind: EntryType::File,
        metadata: Some(entry_metadata.clone()),
    };

    repo.create("source", &entry)?;
    repo.extract("source", &dest_path)?;
    let dest_metadata = dest_path.metadata()?;

    assert_that!(dest_metadata.modified()).is_ok_containing(entry_metadata.modified);
    assert_that!(dest_metadata.accessed()).is_ok_containing(entry_metadata.accessed);

    Ok(())
}

#[rstest]
#[cfg(all(unix, feature = "file-metadata"))]
fn read_common_metadata(
    mut repo: FileRepo<NoSpecial, CommonMetadata>,
    temp_dir: TempDir,
) -> anyhow::Result<()> {
    let source_path = temp_dir.as_ref().join("source");
    File::create(&source_path)?;

    repo.archive(&source_path, "dest")?;
    let entry = repo.entry("dest")?;
    let entry_metadata = entry.metadata.unwrap();
    let source_metadata = source_path.metadata()?;

    assert_that!(entry_metadata.modified).is_equal_to(source_metadata.modified()?);

    Ok(())
}

#[rstest]
fn entries_removed_on_rollback(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create("file", &Entry::file())?;

    let mut object = repo.open("file")?;
    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    repo.rollback()?;

    assert_that!(repo.exists("file")).is_false();

    Ok(())
}

#[rstest]
fn clear_instance_removes_paths(mut repo: FileRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    repo.create("test", &Entry::file())?;
    let mut object = repo.open("test")?;
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    repo.clear_instance();

    assert_that!(repo.exists("test")).is_false();
    assert_that!(repo.open("test")).is_err_variant(acid_store::Error::NotFound);

    Ok(())
}

#[rstest]
fn rollback_after_clear_instance(mut repo: FileRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    repo.create("test", &Entry::file())?;
    let mut object = repo.open("test")?;
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    repo.commit()?;
    repo.clear_instance();
    repo.rollback()?;

    assert_that!(repo.exists("test")).is_true();
    assert_that!(repo.open("test")).is_ok();

    Ok(())
}

#[rstest]
fn verify_valid_repository_is_valid(mut repo: FileRepo) -> anyhow::Result<()> {
    repo.create_parents("home/lostatc/file", &Entry::file())?;

    assert_that!(repo.verify()).is_ok_containing(HashSet::new());

    Ok(())
}
