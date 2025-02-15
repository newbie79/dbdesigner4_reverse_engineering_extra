# dbdesigner4_reverse_engineering_extra
DBDesigner4 comment 추가

### poetry 설치
아래 명령어들을 powershell(관리자로 실행)에서 순서대로 실행한다.
```
mkdir C:\Python\poetry
```
```
$env:POETRY_HOME = 'C:\Python\poetry'
```
```
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```
시스템 환경 변수의 `Path`값에 아래 Poetry 설치경로를 추가한다.  
`C:\Python\poetry\bin`  

### 개발환경 구축 시 poetry를 이용하여 의존성 패키지 가져오기
cmd에서 프로젝트 폴더로 이동 후, 아래 명령어를 실행하여 가상환경이 프로젝트 내에 위치하도록 설정한다.
```
poetry config virtualenvs.in-project true
```
cmd에서 아래 명령어를 실행하여 가상환경을 생성한다.
```
poetry env use python3
```
cmd에서 아래 명령어를 실행하여 기존 패키지들을 가져온다.
```
poetry install
```
cmd에서 아래 명령어를 실행하여 가상환경 정보를 확인한다.
```
poetry env info
```

### 패키지 설치 방법
cmd에서 아래 명령어를 실행하여 가상환경으로 들어간다.
```
poetry shell
```
cmd에서 아래와 같이 패키지를 설치한다. (아래는 fastapi 패키지를 설치하는 예시)
```
(xxxxx) D:\Projects\dbdesigner4_reverse_engineering_extra>poetry add fastapi
```
