# Basic scratchpad for snippets

---
```
@echo off
setlocal enabledelayedexpansion

REM ============================================================
REM List inline policies attached to an IAM role and extract
REM the full policy document (JSON) for each one.
REM
REM Usage:  list_role_policies.bat <role-name> [profile]
REM Example: list_role_policies.bat MyAppRole
REM          list_role_policies.bat MyAppRole my-aws-profile
REM ============================================================

if "%~1"=="" (
    echo Usage: %~nx0 ^<role-name^> [aws-profile]
    exit /b 1
)

set "ROLE_NAME=%~1"
set "PROFILE_ARG="
if not "%~2"=="" set "PROFILE_ARG=--profile %~2"

echo.
echo ============================================================
echo  Role: %ROLE_NAME%
echo ============================================================

REM --- Inline (embedded) policies ---
echo.
echo --- Inline Policies ---
echo.

for /f "usebackq delims=" %%P in (`aws iam list-role-policies --role-name %ROLE_NAME% %PROFILE_ARG% --query "PolicyNames[]" --output text`) do (
    echo [Inline Policy] %%P
    echo --------------------------------------------------------
    aws iam get-role-policy --role-name %ROLE_NAME% --policy-name "%%P" %PROFILE_ARG% --output json
    echo.
)

REM --- Customer-managed attached policies (skip AWS-managed) ---
echo.
echo --- Attached Customer-Managed Policies ---
echo.

for /f "usebackq delims=" %%A in (`aws iam list-attached-role-policies --role-name %ROLE_NAME% %PROFILE_ARG% --query "AttachedPolicies[].PolicyArn" --output text`) do (
    REM Skip AWS-managed policies (arn:aws:iam::aws:policy/...)
    echo %%A | findstr /C:":aws:policy" >nul 2>&1
    if errorlevel 1 (
        echo [Customer Policy] %%A

        REM Get the default version id
        for /f "usebackq delims=" %%V in (`aws iam get-policy --policy-arn "%%A" %PROFILE_ARG% --query "Policy.DefaultVersionId" --output text`) do (
            echo   Version: %%V
            echo --------------------------------------------------------
            aws iam get-policy-version --policy-arn "%%A" --version-id "%%V" %PROFILE_ARG% --output json
            echo.
        )
    )
)

echo ============================================================
echo  Done.
echo ============================================================

endlocal
```

