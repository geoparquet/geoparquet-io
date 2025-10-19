# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

We take the security of geoparquet-io seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### Please do NOT:

- Open a public GitHub issue for security vulnerabilities
- Disclose the vulnerability publicly before it has been addressed

### Please DO:

1. **Report via GitHub Security Advisories** (preferred):
   - Go to the [Security tab](https://github.com/cholmes/geoparquet-io/security/advisories)
   - Click "Report a vulnerability"
   - Fill in the details

2. **Email**: If you prefer, you can email the maintainers at:
   - cholmes@9eo.org
   - Use subject line: "SECURITY: [brief description]"

### What to include:

- Type of vulnerability (e.g., SQL injection, path traversal, etc.)
- Full paths of source file(s) related to the vulnerability
- Location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

### What to expect:

- **Initial Response**: Within 48 hours
- **Status Update**: Within 7 days with our evaluation
- **Fix Timeline**: Critical issues within 30 days, others within 90 days
- **Credit**: We will acknowledge your contribution in the release notes (unless you prefer to remain anonymous)

## Security Best Practices for Users

When using geoparquet-io:

1. **Keep Dependencies Updated**
   - Regularly update to the latest version
   - Run `uv sync` or `pip install --upgrade geoparquet-io`
   - Monitor security advisories

2. **Input Validation**
   - Validate file paths before processing
   - Be cautious when processing files from untrusted sources
   - Use the `--dry-run` flag to preview operations

3. **File Permissions**
   - Ensure proper file permissions on output directories
   - Don't run with elevated privileges unless necessary
   - Be aware of symlink attacks when processing files

4. **Remote Files**
   - Verify URLs before processing remote files
   - Use HTTPS when possible
   - Be cautious with credentials in URLs

5. **Environment**
   - Use virtual environments to isolate dependencies
   - Don't commit sensitive data to version control
   - Review generated files before sharing

## Known Security Considerations

### File Processing
- This tool reads and writes files to disk
- Users should validate file paths and permissions
- Processing untrusted files may expose system vulnerabilities

### DuckDB Spatial Extension
- The spatial extension is loaded for geometry operations
- Keep DuckDB updated to get security patches

### Remote File Access
- The tool can access remote files via HTTP/HTTPS
- Exercise caution with untrusted URLs
- Network requests are made to user-specified locations

## Disclosure Policy

- Security vulnerabilities will be disclosed after a fix is available
- We will publish a security advisory on GitHub
- Critical vulnerabilities will be highlighted in release notes
- CVE IDs will be requested for significant vulnerabilities

## Security Updates

Security updates are delivered through:
- GitHub Security Advisories
- Release notes in CHANGELOG.md
- PyPI package updates
- GitHub Releases

## Questions?

If you have questions about security that are not vulnerabilities, please:
- Open a regular GitHub issue
- Use GitHub Discussions
- Contact maintainers via email

Thank you for helping keep geoparquet-io and its users safe!
