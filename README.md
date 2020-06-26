# qianxunke-fastdfs是一个基于http协议的分布式文件系统，它具有高性能、高可靠、无中心、免维护等优点。

支持多数据库（sqlite，mysql，postgres）灵活切换，可精确查询，支持S3同步

- 支持HTTP下载
- 支持多机自动同步
- 支持断点下载
- 支持配置自动生成
- 支持小文件自动合并(减少inode占用)
- 支持秒传
- 支持跨域访问
- 支持一键迁移（搬迁）
- 支持异地备份（特别是小文件1M以下）
- 支持并行体验
- 支持断点续传([tus](https://tus.io/))
- 支持docker部署
- 支持自监控告警
- 支持图片缩放
- 支持自定义认证
- 支持集群文件信息查看
- 使用通用HTTP协议
- 支持s3同步
- 支持动态切换文件元数据数据库

#参考开源软件
[go-fastdfs](https://github.com/sjqzhang/go-fastdfs)

[tus](https://tus.io/)

