---
name: bkit-hooks-guide
description: Claude Code 훅 동작을 Antigravity 환경에서 수동으로 적용하기 위한 가이드
---

# bkit Hooks Guide (Claude Code → Antigravity)

> 원본 bkit-claude-code의 `hooks/hooks.json`에 정의된 이벤트 훅 동작을
> Antigravity 환경에서 수동으로 적용하기 위한 가이드입니다.

---

## 원본 훅 개요

원본 Claude Code 환경에서는 `hooks.json`을 통해 이벤트별 Node.js 스크립트가 자동 실행되었습니다.
Antigravity에서는 이 자동 실행 메커니즘이 없으므로, 아래 내용을 참고하여 수동으로 적용합니다.

## 이벤트별 동작 가이드

### 1. SessionStart (세션 시작)

**원본 스크립트**: `hooks/session-start.js`

**동작 설명**:
- 세션 시작 시 한 번만 실행 (once: true)
- bkit 초기화 수행

**수동 적용 방법**:
- 작업 시작 시 `bkit-rules` 스킬을 참조하여 PDCA 규칙 확인
- 프로젝트 레벨 (Starter/Dynamic/Enterprise) 자동 감지 적용

---

### 2. PreToolUse - Write/Edit (쓰기 전)

**원본 스크립트**: `scripts/pre-write.js`

**동작 설명**:
- 파일 쓰기/편집 전에 실행
- 코딩 컨벤션 검증, 네이밍 규칙 확인

**수동 적용 방법**:
- 파일 작성 전 `phase-2-convention` 스킬의 네이밍 규칙 확인
- CONVENTIONS.md 정의된 규칙 준수 확인

---

### 3. PostToolUse - Write (쓰기 후)

**원본 스크립트**: `scripts/unified-write-post.js`

**동작 설명**:
- 파일 쓰기 완료 후 실행
- 코드 품질 검증, 자동 포맷팅 확인

**수동 적용 방법**:
- 파일 작성 후 `code-review` 스킬 기준으로 코드 품질 확인
- 필요 시 `code-analyzer` 에이전트 스킬 참조

---

### 4. PostToolUse - Bash (명령 실행 후)

**원본 스크립트**: `scripts/unified-bash-post.js`

**동작 설명**:
- Bash 명령 실행 후 로그 분석
- 에러 패턴 감지

**수동 적용 방법**:
- 명령 실행 후 오류 발생 시 `qa-monitor` 에이전트 스킬 참조
- Docker 로그 분석 시 `zero-script-qa` 스킬 활용

---

### 5. PreToolUse - Bash (명령 실행 전)

**원본 스크립트**: `scripts/unified-bash-pre.js`

**동작 설명**:
- Bash 명령 실행 전 안전성 검증
- 위험 명령어 차단

**수동 적용 방법**:
- 명령 실행 전 `security-architect` 에이전트 스킬의 보안 규칙 확인
- 프로덕션 환경 영향 명령어 주의

---

### 6. Stop (세션 종료)

**원본 스크립트**: `scripts/unified-stop.js`

**동작 설명**:
- 세션 종료 시 실행
- PDCA 상태 저장, 진행 요약

**수동 적용 방법**:
- 작업 종료 전 PDCA 상태 확인 (`docs/.pdca-status.json`)
- `report-generator` 에이전트 스킬 참조하여 진행 보고서 작성

---

### 7. UserPromptSubmit (사용자 입력)

**원본 스크립트**: `scripts/prompt-enhancer.js`

**동작 설명**:
- 사용자 프롬프트 제출 시 실행
- 프롬프트 자동 개선, 컨텍스트 추가

**수동 적용 방법**:
- `bkit-rules` 스킬의 자동 트리거 규칙 참조
- 키워드 기반 에이전트 자동 활성화 패턴 적용

---

### 8. PreCompact (컨텍스트 압축 전)

**원본 스크립트**: `scripts/context-snapshot.js`

**동작 설명**:
- 컨텍스트 압축 전 스냅샷 저장

**수동 적용 방법**:
- 장시간 작업 시 중간 상태를 문서화
- PDCA 상태 파일을 주기적으로 업데이트

---

### 9. TaskCompleted (작업 완료)

**원본 스크립트**: `scripts/task-advance.js`

**동작 설명**:
- 작업 완료 시 다음 작업 자동 진행
- PDCA 단계 자동 전환

**수동 적용 방법**:
- 작업 완료 후 `pdca` 스킬의 `/pdca next` 참조
- 다음 PDCA 단계 확인 및 진행

---

### 10. SubagentStart / SubagentStop (팀 에이전트)

**원본 스크립트**: `scripts/teammate-start.js`, `scripts/teammate-cleanup.js`

**동작 설명**:
- Agent Teams 시작/종료 시 실행
- 팀원 초기화 및 정리

**수동 적용 방법**:
- `cto-lead` 에이전트 스킬의 팀 구성 패턴 참조
- Enterprise 프로젝트에서 병렬 작업 시 수동 조율

---

## 스크립트 파일 위치

원본 스크립트 파일들은 `scripts/` 디렉토리에 보존되어 있습니다.
Node.js 환경에서 직접 실행이 필요한 경우 해당 파일을 참조하세요.

```
scripts/
├── pre-write.js
├── unified-write-post.js
├── unified-bash-pre.js
├── unified-bash-post.js
├── unified-stop.js
├── prompt-enhancer.js
├── context-snapshot.js
├── task-advance.js
├── teammate-start.js
├── teammate-cleanup.js
└── ...
```
