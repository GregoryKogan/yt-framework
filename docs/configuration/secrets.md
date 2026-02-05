# Secrets Management

Sensitive data (credentials, tokens, etc.) is stored in `configs/secrets.env`:

```bash
# configs/secrets.env
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token

# Optional: S3 credentials
S3_ENDPOINT=https://your-s3-endpoint.com
S3_DOWNLOAD_ACCESS_KEY=your-download-access-key
S3_DOWNLOAD_SECRET_KEY=your-download-secret-key
# For upload operations:
S3_UPLOAD_ACCESS_KEY=your-upload-access-key
S3_UPLOAD_SECRET_KEY=your-upload-secret-key
```

## YT Credentials

Required for production mode:

```bash
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token
```

### Getting YT Credentials

1. Contact your YTsaurus cluster administrator
2. Request YT proxy URL and authentication token
3. Add credentials to `configs/secrets.env`

## S3 Credentials

Required for S3 integration:

```bash
S3_ENDPOINT=https://your-s3-endpoint.com
S3_DOWNLOAD_ACCESS_KEY=your-download-access-key
S3_DOWNLOAD_SECRET_KEY=your-download-secret-key
# For upload operations:
S3_UPLOAD_ACCESS_KEY=your-upload-access-key
S3_UPLOAD_SECRET_KEY=your-upload-secret-key
```

### S3 Credential Types

- **Download credentials**: For reading/list operations
- **Upload credentials**: For write operations (optional if download credentials have write access)

## Loading Secrets

Secrets are automatically loaded by the framework. Access them in stages:

```python
from yt_framework.utils.env import load_secrets

class MyStage(BaseStage):
    def __init__(self, deps, logger):
        super().__init__(deps, logger)
        
        # Load secrets
        secrets = load_secrets(self.deps.configs_dir)
        yt_proxy = secrets.get("YT_PROXY")
        yt_token = secrets.get("YT_TOKEN")
```

### Environment Variable Reference

| Variable | Required For | Description |
|----------|--------------|-------------|
| `YT_PROXY` | Prod mode | YTsaurus cluster proxy URL |
| `YT_TOKEN` | Prod mode | YTsaurus authentication token |
| `S3_ENDPOINT` | S3 operations | S3 service endpoint URL |
| `S3_DOWNLOAD_ACCESS_KEY` | S3 read | S3 access key for read operations |
| `S3_DOWNLOAD_SECRET_KEY` | S3 read | S3 secret key for read operations |
| `S3_UPLOAD_ACCESS_KEY` | S3 write | S3 access key for write operations |
| `S3_UPLOAD_SECRET_KEY` | S3 write | S3 secret key for write operations |
| `DOCKER_AUTH_USERNAME` | Private Docker | Docker registry username |
| `DOCKER_AUTH_PASSWORD` | Private Docker | Docker registry password |

## Security Best Practices

```{warning}
**Never commit secrets to version control!**
```

1. **Never commit secrets**: Add `configs/secrets.env` to `.gitignore`
2. **Use example files**: Create `configs/secrets.example.env` with placeholder values
3. **Rotate credentials**: Regularly update tokens and keys
4. **Use environment variables**: In CI/CD, use environment variables instead of files
5. **Limit access**: Restrict file permissions on secrets.env (chmod 600)

### Example .gitignore Entry

```gitignore
# Secrets
configs/secrets.env
*.env
!*example.env
```

### Example secrets.example.env

```bash
# configs/secrets.example.env
YT_PROXY=your-yt-proxy-url
YT_TOKEN=your-yt-token

# S3 credentials (optional)
S3_ENDPOINT=https://your-s3-endpoint.com
S3_DOWNLOAD_ACCESS_KEY=your-download-access-key
S3_DOWNLOAD_SECRET_KEY=your-download-secret-key
S3_UPLOAD_ACCESS_KEY=your-upload-access-key
S3_UPLOAD_SECRET_KEY=your-upload-secret-key
```

## CI/CD Integration

In CI/CD pipelines, use environment variables instead of files:

```bash
# Set environment variables
export YT_PROXY="your-proxy"
export YT_TOKEN="your-token"

# Run pipeline
python pipeline.py
```

The framework will automatically load secrets from environment variables if `secrets.env` is not found.

## Troubleshooting

### Secrets not loading

- Verify `configs/secrets.env` exists
- Check file permissions
- Review file format (KEY=VALUE, one per line)
- Check for syntax errors

### Credentials invalid

- Verify credentials are correct
- Check token expiration
- Review YT/S3 permissions
- Test credentials manually

## See Also

- [Configuration Guide](index.md) - Complete configuration reference
- [Dev vs Prod](../dev-vs-prod.md) - Understanding when secrets are required
- [Troubleshooting](../troubleshooting/configuration.md) - Common secrets issues
