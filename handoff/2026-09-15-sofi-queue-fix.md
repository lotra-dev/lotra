# SoFi rejection queue fix — 2026-09-15

## Observed cause

The running desktop log recorded 19 NCT order rejections from SoFi:
`Security 'EQUITY-NCT' cannot be traded` (HTTP 400). Each account was attempted
even after the first security-wide rejection. At 11:37:11 ET, the run reached
its processing limit with 41 submissions; 20 Robinhood/WellsTrade orders were
deferred. Those 20 submissions subsequently completed at 12:01:19 ET.

IBO completed desktop processing at 11:53:41 ET with 47 submissions and no
failures. Read-only local order-history counts: Public 10, Fennel 17,
Robinhood 10, WellsTrade 10. All 47 records were SUBMITTED at inspection;
this is not a fresh broker fill confirmation.

## Change

- After the first definitive SoFi rejection stating that the selected security
  cannot be traded, skip remaining SoFi orders for that ticker in this run,
  including accounts on other logins.
- Preserve the rejection and per-account skip reasons in the Activity Log.
- Continue other tickers and brokers. Skip fully affected SoFi logins before
  connecting, and exclude these terminal skips from automatic retry targets,
  including when the overall processing deadline has elapsed.
- Keep account-specific rejections isolated to that account. Keep uncertain
  submissions protected from automatic retry. The symbol skip is not persisted
  as a permanent restriction.

Implementation: `localrsa/ui/execute_page.py`.
Regression coverage: `tests/test_execute_progress.py`.

## Validation

163 tests passed across execution progress, cloud signal scheduling,
execution safety, execution guards, SoFi sessions, and Activity Log behavior.
An additional timeout regression passed: known SoFi security skips remain
terminal across later logins even after the overall batch deadline expires.
Ruff lint and formatting checks passed for both changed Python files.
Tests used isolated data and fake broker adapters. No live orders were placed,
cancelled, or retried by this task.

## Windows artifact

The local hotfix build uses version 1.21.16 in the separate output directory
`release-nuitka-v1.21.16-sofi-queue-fix`. It does not update the published
release, Cloudflare configuration, or the canonical `latest` executable.
The existing running executable needs to be closed and the patched copy opened
to use the fix.

Build completed successfully. The packaging scan checked 717 files; Microsoft
Defender reported no threats. Packaged `--verify-broker-packages` and `--no-ui`
startup checks both exited successfully using temporary app data.

- File: `release-nuitka-v1.21.16-sofi-queue-fix/Lotra.exe`
- Size: 203579392 bytes
- SHA256: `E66A6866BFF7194D95B151805820E5CA077AD97BF278FADCECC3932FEFD6BB7E`
- Build log: `build-sofi-queue-fix-20260915.log`
