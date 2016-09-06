# n.io File Persistence Module

A n.io module implementing file based state saving and restoration.

## Configuration

[persistence]

Location to save persistence file.
- data=etc/persist/

## Dependencies

- None

## Usage
This repo must be checked out (`git submodule`) or linked (`ln -s`) into a user's `niocore/modules/persistence directory` as `file`. Any other directory name will break the import paths.
