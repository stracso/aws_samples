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

REM Write policy names to a temp file, one per line
aws iam list-role-policies --role-name %ROLE_NAME% %PROFILE_ARG% --query "PolicyNames[]" --output json > "%TEMP%\inline_policies.json"

REM Parse each policy name from the JSON array
for /f "usebackq tokens=* delims=" %%P in (`type "%TEMP%\inline_policies.json" ^| findstr /v /r "^\[" ^| findstr /v /r "^\]"`) do (
    REM Strip quotes, commas, and leading spaces
    set "RAW=%%P"
    set "RAW=!RAW:"=!"
    set "RAW=!RAW:,=!"
    for /f "tokens=* delims= " %%T in ("!RAW!") do set "POLICY_NAME=%%T"

    if not "!POLICY_NAME!"=="" (
        echo [Inline Policy] !POLICY_NAME!
        echo --------------------------------------------------------
        aws iam get-role-policy --role-name %ROLE_NAME% --policy-name "!POLICY_NAME!" %PROFILE_ARG% --output json
        echo.
    )
)

REM --- Customer-managed attached policies (skip AWS-managed) ---
echo.
echo --- Attached Customer-Managed Policies ---
echo.

REM Write attached policy ARNs to a temp file, one per line
aws iam list-attached-role-policies --role-name %ROLE_NAME% %PROFILE_ARG% --query "AttachedPolicies[].PolicyArn" --output json > "%TEMP%\attached_policies.json"

for /f "usebackq tokens=* delims=" %%A in (`type "%TEMP%\attached_policies.json" ^| findstr /v /r "^\[" ^| findstr /v /r "^\]"`) do (
    set "RAW=%%A"
    set "RAW=!RAW:"=!"
    set "RAW=!RAW:,=!"
    for /f "tokens=* delims= " %%T in ("!RAW!") do set "POLICY_ARN=%%T"

    if not "!POLICY_ARN!"=="" (
        REM Skip AWS-managed policies
        echo !POLICY_ARN! | findstr /C:":aws:policy" >nul 2>&1
        if errorlevel 1 (
            echo [Customer Policy] !POLICY_ARN!

            REM Get the default version id
            for /f "usebackq delims=" %%V in (`aws iam get-policy --policy-arn "!POLICY_ARN!" %PROFILE_ARG% --query "Policy.DefaultVersionId" --output text`) do (
                echo   Version: %%V
                echo --------------------------------------------------------
                aws iam get-policy-version --policy-arn "!POLICY_ARN!" --version-id "%%V" %PROFILE_ARG% --output json
                echo.
            )
        )
    )
)

REM Cleanup temp files
del "%TEMP%\inline_policies.json" 2>nul
del "%TEMP%\attached_policies.json" 2>nul

echo ============================================================
echo  Done.
echo ============================================================

endlocal


```

