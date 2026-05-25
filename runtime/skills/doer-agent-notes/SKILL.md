---
name: doer-agent-notes
description: Use when working with Doer agent text notes, memo files, note patch history, or note CRUD/search tools.
---

# Doer Agent Notes

Use the Doer notes tools for note CRUD whenever they are available. Do not edit `.doer-agent/notes/*.md` directly when a notes tool can perform the same operation, because note tools also record patch history.

## Storage

Current note files live directly under `.doer-agent/notes` as Markdown files.

- Notes: `.doer-agent/notes/{note-id}.md`
- There is no built-in default note filename. Users choose the filename when creating a note.
- `note-id` is the filename. If the extension is omitted, normalize it back to `.md`.
- Note content is free-form Markdown text.
- Titles, tags, and frontmatter are optional; do not require them for storage.

## Tools

Prefer these tools when present:

- `notes_list`
- `notes_read`
- `notes_create`
- `notes_save`
- `notes_rename`
- `notes_delete`
- `notes_search`

## Patch History

Note change history is append-only and stored under `.doer-agent/notes/patches/YYYY/MM/*.patch`.

Patch files use git diff format with Doer metadata comment lines before the diff body.

For content changes, include:

- `# doer-note-patch-id`
- `# created-at`
- `# operation: content`
- `# note-id`
- `# note-path`
- `# base-sha256`
- `# next-sha256`

For filename changes, include:

- `# doer-note-patch-id`
- `# created-at`
- `# operation: rename`
- `# note-id`
- `# note-path`
- `# next-note-id`
- `# next-note-path`

For deletions, include:

- `# doer-note-patch-id`
- `# created-at`
- `# operation: delete`
- `# note-id`
- `# note-path`
- `# base-sha256`
- `# next-sha256`

## Principles

- The note source of truth is human-readable `.md` files.
- Change history is append-only `.patch` files.
- Search indexes, vector databases, and other derived stores are not source of truth.
- Derived data must be reproducible from `.md` and `.patch` files.
