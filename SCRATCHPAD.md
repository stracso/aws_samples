# Basic scratchpad for snippets

---
```

@echo off
setlocal enabledelayedexpansion

REM ============================================================
REM List inline and customer-managed policies for an IAM role,
REM and extract the full policy document JSON for each.
REM
REM Usage:  list_role_policies.bat <role-name> [aws-profile]
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

REM ===========================================
REM  INLINE POLICIES
REM ===========================================
echo.
echo --- Inline Policies ---
echo.

REM Get the count of inline policies
for /f %%C in ('aws iam list-role-policies --role-name %ROLE_NAME% %PROFILE_ARG% --query "length(PolicyNames)" --output text') do set "INLINE_COUNT=%%C"

if "!INLINE_COUNT!"=="0" (
    echo   No inline policies found.
) else (
    set /a "LAST_IDX=!INLINE_COUNT!-1"
    for /l %%I in (0,1,!LAST_IDX!) do (
        for /f "usebackq delims=" %%N in (`aws iam list-role-policies --role-name %ROLE_NAME% %PROFILE_ARG% --query "PolicyNames[%%I]" --output text`) do (
            echo [Inline Policy %%I] %%N
            echo --------------------------------------------------------
            aws iam get-role-policy --role-name %ROLE_NAME% --policy-name "%%N" %PROFILE_ARG% --output json
            echo.
        )
    )
)

REM ===========================================
REM  ATTACHED CUSTOMER-MANAGED POLICIES
REM ===========================================
echo.
echo --- Attached Customer-Managed Policies ---
echo.

REM Get total count of attached policies
for /f %%C in ('aws iam list-attached-role-policies --role-name %ROLE_NAME% %PROFILE_ARG% --query "length(AttachedPolicies)" --output text') do set "ATTACHED_COUNT=%%C"

if "!ATTACHED_COUNT!"=="0" (
    echo   No attached policies found.
) else (
    set /a "LAST_IDX=!ATTACHED_COUNT!-1"
    for /l %%I in (0,1,!LAST_IDX!) do (
        for /f "usebackq delims=" %%A in (`aws iam list-attached-role-policies --role-name %ROLE_NAME% %PROFILE_ARG% --query "AttachedPolicies[%%I].PolicyArn" --output text`) do (
            REM Skip AWS-managed policies
            echo %%A | findstr /C:"arn:aws:iam::aws:policy" >nul 2>&1
            if errorlevel 1 (
                echo [Customer Policy %%I] %%A

                for /f "usebackq delims=" %%V in (`aws iam get-policy --policy-arn "%%A" %PROFILE_ARG% --query "Policy.DefaultVersionId" --output text`) do (
                    echo   Version: %%V
                    echo --------------------------------------------------------
                    aws iam get-policy-version --policy-arn "%%A" --version-id "%%V" %PROFILE_ARG% --output json
                    echo.
                )
            )
        )
    )
)

echo ============================================================
echo  Done.
echo ============================================================
endlocal



```

