---
description: bkit Output Style 설치 워크플로우
---

# Output Style Setup

bkit의 Output Style을 설치합니다.

## Available Styles

| Style | Recommended For | Description |
|-------|----------------|-------------|
| bkit-learning | Starter | 학습 모드 - PDCA를 배우며 개발 |
| bkit-pdca-guide | Dynamic | PDCA 워크플로우 가이드 + 자동 체크리스트 |
| bkit-enterprise | Enterprise | CTO 관점 아키텍처/보안/성능 분석 |
| bkit-pdca-enterprise | Enterprise | PDCA + CTO 통합 (가장 상세) |

## Setup Steps

1. 사용자에게 project level 또는 user level 설치를 확인합니다
2. **Project level** (현재 프로젝트만):
   - `output-styles/*.md` 파일을 프로젝트의 적절한 위치로 복사
3. **User level** (모든 프로젝트):
   - `output-styles/*.md` 파일을 사용자 홈 디렉토리의 적절한 위치로 복사
4. 설치 완료 후 파일 목록으로 확인
5. 프로젝트 레벨 감지 결과에 따라 추천 스타일 안내

## Post-Setup

설치 후 안내사항:
- 설치된 스타일 중 선택하여 활성화
- 프로젝트 레벨 감지에 따른 추천 스타일
- 세션 중 언제든 스타일 변경 가능

---

## 참고

이 워크플로우는 원본 bkit-claude-code의 `/output-style-setup` 명령어에서 변환되었습니다.
`${CLAUDE_PLUGIN_ROOT}` 경로는 상대 경로로 대체됩니다.
