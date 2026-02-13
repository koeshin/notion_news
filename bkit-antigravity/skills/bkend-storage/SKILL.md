---
name: bkend-storage
description: bkend.ai file storage expert skill.
---


# bkend.ai Storage Guide

## Upload Methods

| Method | Use Case | Process |
|--------|----------|---------|
| Single | Normal files | Presigned URL -> PUT upload -> Register metadata |
| Multiple | Multiple files | Repeat single upload |
| Multipart | Large files | Initialize -> Part URLs -> Complete |

## Presigned URL

- Validity: 15 minutes
- PUT method with file binary
- Content-Type header required

## File Visibility (4 levels)

| Level | Access | URL Type |
|-------|--------|----------|
| public | Anyone | CDN URL (no expiry) |
| private | Owner only | Presigned URL (1 hour) |
| protected | Authenticated users | Presigned URL (1 hour) |
| shared | Specified targets | Presigned URL (1 hour) |

## Size Limits

| Category | Max Size |
|----------|----------|
| Images | 10 MB |
| Videos | 100 MB |
| Documents | 20 MB |

## Storage Categories

images, documents, media, attachments

## REST Storage API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /v1/files/presigned-url | Generate presigned URL |
| POST | /v1/files | Register metadata (complete upload) |
| GET | /v1/files | File list |
| GET | /v1/files/{id} | File detail |
| PATCH | /v1/files/{id} | Update metadata |
| DELETE | /v1/files/{id} | Delete file |
| GET | /v1/files/{id}/download-url | Generate download URL |

## MCP Storage Tool

| Tool | Purpose |
|------|---------|
| 7_code_examples_data | CRUD + file upload code examples |

## Upload Flow (Single File)

```
1. POST /v1/files/presigned-url -> { url, fileId }
2. PUT {url} with file binary + Content-Type header
3. POST /v1/files with { fileId, filename, contentType, size, visibility }
```

## Multipart Upload Flow (Large File)

```
1. POST /v1/files/multipart/init -> { uploadId }
2. POST /v1/files/multipart/urls -> [{ partNumber, url }]
3. PUT each part URL with file chunk
4. POST /v1/files/multipart/complete -> { file }
```

## Official Documentation (Live Reference)

For the latest storage documentation, use WebFetch:
- Full TOC: https://raw.githubusercontent.com/popup-studio-ai/bkend-docs/main/SUMMARY.md
- Storage: https://raw.githubusercontent.com/popup-studio-ai/bkend-docs/main/src/storage/

---

## Claude Code 원본 참조 정보

- **원본 에이전트**: bkit:bkend-expert
- **사용자 호출 가능**: false
- **임포트**: ./templates/shared/bkend-patterns.md
