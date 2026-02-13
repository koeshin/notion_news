# bkit-antigravity

> bkit v1.5.3를 Antigravity 환경으로 변환한 버전입니다.

## 개요

bkit(비킷)은 AI Native 개발을 위한 Context Engineering 도구입니다.
PDCA(Plan-Design-Do-Check-Act) 방법론과 9단계 Development Pipeline을 통해
체계적인 개발 프로세스를 제공합니다.

이 폴더는 원본 `bkit-claude-code` 플러그인의 모든 콘텐츠를 보존하면서,
Antigravity 환경에 맞게 실행 방식을 변환한 버전입니다.

## 변환 내용

| 원본 (Claude Code) | 변환 후 (Antigravity) | 설명 |
|---|---|---|
| `agents/*.md` (16개) | `.skills/agents/{name}/SKILL.md` | 에이전트 → 스킬 변환 |
| `skills/*/SKILL.md` (26개) | `skills/*/SKILL.md` | 프론트매터 정리 |
| `commands/*.md` (2개) | `.agent/workflows/*.md` | 명령어 → 워크플로우 변환 |
| `hooks/hooks.json` + scripts | `.skills/bkit-hooks-guide/SKILL.md` | 훅 동작 → 가이드 문서화 |
| `.claude-plugin/plugin.json` | (제외) | 플러그인 매니페스트 불필요 |

### 변경되지 않은 파일

아래 파일/디렉토리는 원본 그대로 복사되었습니다:

- `templates/` - PDCA 문서 템플릿
- `output-styles/` - Output Style 정의
- `docs/` - 프로젝트 문서
- `bkit-system/` - 시스템 관련 파일
- `lib/` - 라이브러리 코드
- `scripts/` - Node.js 스크립트 (참조용)
- `refs/` - 참조 파일
- `images/` - 이미지 에셋
- `bkit.config.json` - 메인 설정 파일
- `CONVENTIONS.md` - 코딩 컨벤션
- `CHANGELOG.md` - 변경 이력

## 구조

```
bkit-antigravity/
├── .agent/workflows/           # Antigravity 워크플로우 (명령어 변환)
│   ├── bkit.md                 # bkit 도움말
│   └── output-style-setup.md   # Output Style 설치
├── .skills/
│   ├── agents/                 # 에이전트 → 스킬 변환 (16개)
│   │   ├── cto-lead/SKILL.md
│   │   ├── code-analyzer/SKILL.md
│   │   ├── gap-detector/SKILL.md
│   │   └── ...
│   └── bkit-hooks-guide/       # 훅 동작 가이드
│       └── SKILL.md
├── skills/                     # 기존 스킬 (26개, 프론트매터 정리)
│   ├── pdca/SKILL.md
│   ├── bkit-rules/SKILL.md
│   └── ...
├── templates/                  # PDCA 문서 템플릿
├── output-styles/              # Output Style 정의
├── scripts/                    # Node.js 스크립트 (참조용)
├── docs/                       # 프로젝트 문서
├── bkit-system/                # 시스템 파일
├── lib/                        # 라이브러리
├── refs/                       # 참조 파일
├── images/                     # 이미지
├── hooks/                      # 원본 훅 정의 (참조용)
├── bkit.config.json            # 메인 설정
├── CONVENTIONS.md              # 코딩 컨벤션
└── CHANGELOG.md                # 변경 이력
```

## PDCA 사용법

### 기본 워크플로우

1. **Plan**: 기능 계획 문서 작성 → `skills/pdca/SKILL.md` 참조
2. **Design**: 설계 문서 작성 → `.skills/agents/design-validator/SKILL.md` 참조
3. **Do**: 구현 → 프로젝트 레벨별 에이전트 스킬 참조
4. **Check**: 갭 분석 → `.skills/agents/gap-detector/SKILL.md` 참조
5. **Act**: 반복 개선 → `.skills/agents/pdca-iterator/SKILL.md` 참조

### 프로젝트 레벨

| 레벨 | 대상 | 참조 스킬 |
|------|------|-----------|
| Starter | 초보/비개발자 | `skills/starter/SKILL.md` |
| Dynamic | 중급 (BaaS 활용) | `skills/dynamic/SKILL.md` |
| Enterprise | 고급 (마이크로서비스) | `skills/enterprise/SKILL.md` |

## Claude Code와의 차이점

- **에이전트 시스템**: Claude Code의 Task 기반 에이전트 호출 대신, 스킬 문서로 참조
- **훅 시스템**: 자동 이벤트 훅 대신, `.skills/bkit-hooks-guide/SKILL.md`의 수동 적용 가이드
- **명령어**: `/command` 대신, `.agent/workflows/` 워크플로우 참조
- **메모리**: Claude Code의 agent memory 대신, 프로젝트 문서 기반 상태 관리

## 원본 버전

원본: bkit-claude-code v1.5.3
변환 대상: Antigravity 환경
