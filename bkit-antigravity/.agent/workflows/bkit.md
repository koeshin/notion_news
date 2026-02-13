---
description: bkit 도움말 - 사용 가능한 모든 bkit 기능 목록
---

# bkit 도움말

> 사용 가능한 모든 bkit 기능을 나열합니다.

## PDCA 워크플로우

| 명령어 | 설명 |
|--------|------|
| `/pdca plan {feature}` | 기능 계획 문서 생성 |
| `/pdca design {feature}` | 설계 문서 생성 |
| `/pdca do {feature}` | 구현 시작 |
| `/pdca analyze {feature}` | 설계-구현 갭 분석 |
| `/pdca iterate {feature}` | 자동 반복 개선 |
| `/pdca report {feature}` | 완료 보고서 생성 |
| `/pdca archive {feature}` | PDCA 결과 보관 |
| `/pdca cleanup {feature}` | 작업 정리 |
| `/pdca team {feature}` | Agent Teams 모드 시작 |
| `/pdca status` | 현재 PDCA 상태 확인 |
| `/pdca next` | 다음 단계 가이드 |

## 프로젝트 초기화

| 명령어 | 설명 |
|--------|------|
| `init starter` | Starter 프로젝트 초기화 (HTML/CSS/JS) |
| `init dynamic` | Dynamic 프로젝트 초기화 (bkend.ai BaaS) |
| `init enterprise` | Enterprise 프로젝트 초기화 |

## Development Pipeline

| 단계 | 설명 |
|------|------|
| Phase 1 | Schema/용어 정의 |
| Phase 2 | 코딩 컨벤션 |
| Phase 3 | 목업 개발 |
| Phase 4 | API 설계/구현 |
| Phase 5 | 디자인 시스템 |
| Phase 6 | UI 구현 + API 연동 |
| Phase 7 | SEO/보안 |
| Phase 8 | 리뷰 |
| Phase 9 | 배포 |

## 품질 관리

| 명령어 | 설명 |
|--------|------|
| `code review` | 코드 리뷰 요청 |
| `zero script qa` | 로그 기반 QA |
| `security scan` | 보안 스캔 |
| `gap analysis` | 설계-구현 갭 분석 |

## 학습

| 명령어 | 설명 |
|--------|------|
| `learn` | 학습 모드 시작 |
| `setup` | 환경 설정 가이드 |

## Output Styles (v1.5.2)

| 명령어 | 설명 |
|--------|------|
| `/output-style-setup` | Output Style 설치 |
| `/output-style {name}` | Output Style 적용 |

사용 가능한 스타일:
- `bkit-learning`: Starter용 학습 모드
- `bkit-pdca-guide`: Dynamic용 PDCA 가이드
- `bkit-enterprise`: Enterprise용 CTO 관점 분석
- `bkit-pdca-enterprise`: PDCA + CTO 통합 (가장 상세)

## Agent Teams (v1.5.1)

Dynamic: 2인 팀 (developer + reviewer)
Enterprise: 4인 팀 (leader + developer + reviewer + qa)

## v1.5.3 신규 기능

### Output Styles
프로젝트 레벨에 따른 자동 추천. 레벨별 최적화된 출력 형식 제공.

### Agent Teams
PDCA 병렬 실행. Dynamic은 2인, Enterprise는 4인 팀 자동 구성.

### Agent Memory
에이전트 간 학습 기억 공유. `project` 범위 또는 `user` 범위 선택 가능.

---

## 참고

이 워크플로우는 원본 bkit-claude-code의 `/bkit` 명령어에서 변환되었습니다.
Antigravity 환경에서는 `/bkit` 대신 이 워크플로우 파일을 참조합니다.
