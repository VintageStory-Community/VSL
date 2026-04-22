# VSL

Vintage Story 服务器启动器（Windows / WPF）。

## 功能概览

- 版本安装与本地 ZIP 导入
- 档案管理与配置编辑
- 存档管理与 Mod 管理
- 控制台日志、命令发送、日志下载

## 运行与构建

```powershell
dotnet build VSL.sln -c Release
```

## 打包

使用 GitHub Actions 工作流：

- 工作流文件：`.github/workflows/build-packages.yml`
- 手动触发：`Actions -> Build Packages -> Run workflow`
- 手动触发输入：
  - `version`：版本号（如 `1.1.10`）
  - `runtime`：运行时（默认 `win-x64`）
- 产物：
  - 便携版：`VSL-<version>-<runtime>.zip`
  - 安装版：`VSL-Setup-<version>-<runtime>.exe`
  - 校验文件：`SHA256SUMS.txt`
- 推送标签（如 `v1.1.10`）会自动创建 Release 并上传上述文件。

## 数据目录策略

- 便携版：包内包含 `portable.mode` 标记文件，默认使用程序目录下 `workspace`。
- 安装版：默认使用 `%LocalAppData%\\VSL\\workspace`（通常在 C 盘用户目录）。
- 卸载安装版时会询问是否删除用户数据（存档、配置、日志）。

## 关于

- 作者：寒士杰克（HansJack）
- 制作组：复古物语中文社区（vintagestory.top）
- 项目仓库：https://github.com/VintageStory-Community/VSL
