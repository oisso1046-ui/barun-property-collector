# 바른부동산 매물 자동수집

GitHub Actions에서 매일 오전 9시 10분(KST)에 실행하고 Google Sheets에 기록합니다.

## 저장소에 들어갈 파일

- `main.py`
- `requirements.txt`
- `.github/workflows/collect.yml`

## GitHub Secrets

저장소 → Settings → Secrets and variables → Actions → New repository secret

### 1. GOOGLE_SHEET_ID

구글시트 주소에서 `/d/`와 `/edit` 사이의 값입니다.

현재 사용 중인 시트 예시:

```text
1Ax_z5OcACNd2Eb2Th19MD0LVgDR6O2dMU68hJmTPFJE
```

실제 주소와 같은지 확인한 뒤 저장하세요.

### 2. GOOGLE_SERVICE_ACCOUNT_JSON

Google Cloud에서 서비스 계정 키(JSON)를 만든 뒤 파일 내용 전체를 Secret 값으로 붙여넣습니다.

서비스 계정 이메일을 해당 구글시트의 공유 대상에 **편집자**로 추가해야 합니다.

## 최초 테스트

GitHub 저장소 → Actions → `바른부동산 매물 자동수집` → Run workflow

실행 결과와 상세 오류는 Google Sheets의 `실행로그` 탭에도 기록됩니다.

## 현재 단지번호

- 더샵데시앙 1단지: 132577
- 더샵데시앙 3차: 141426
- 더샵데시앙 4단지: 125854
- 사송트루엘: 129865
- 사송에비뉴원: 151001
- 사송제일풍경채: 144883

코드는 구글시트의 `단지번호관리` 탭을 우선 읽으며, 값이 없을 때만 위 기본 목록을 사용합니다.

## 주의

네이버 내부 API 구조나 접근 정책이 바뀌면 코드 수정이 필요할 수 있습니다.  
401·403은 세션을 재초기화한 뒤 정해진 횟수만 재시도하고, 계속 거부되면 종료합니다.
