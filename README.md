# Playnite <-> Grist Sync

用于 Playnite 和 Grist 之间的双向同步：

- P2G: 把 Playnite 游戏库同步到 Grist（以 `playniteId` 为主键 upsert）。
- G2P: 把 Grist 白名单字段回写到 Playnite。
- 支持增量、冲突策略、日志与定时任务调用。

本项目依赖 Playnite HTTP API。需要安装插件：

- https://github.com/rollacode/playnite-bridge

安装后在 Playnite 中获取 API Token，并配置到 `config.yaml` 的 `token` 字段。

## 同步
### P2G（Playnite -> Grist）

P2G 会同步 Playnite 返回的大部分字段，包含常见信息如：

- 标识与基础信息：`playniteId`（由 Playnite `id` 映射）、`name`、`source`、`description`、`notes`
- 分类类字段（列表）：`categories`、`tags`、`features`、`genres`、`developers`、`publishers`、`series`、`platforms`、`ageRatings`、`regions`
- 状态与统计：`completionStatus`、`favorite`、`hidden`、`isInstalled`、`playtime`、`playCount`、`userScore`
- 时间：`added`、`modified`、`lastActivity`、`releaseDate`
- 同步辅助字段：`syncedAt`、`editedAt`

说明：

- `links` 会转换为 Markdown 文本存到 Grist。
- 新插入记录会将 `editedAt` 回填为 `null`，避免首轮初始化误判。

### G2P（Grist -> Playnite）

G2P 默认只回写白名单字段（`g2p_fields`）：

- `description`
- `notes`
- `releaseDate`
- `favorite`
- `hidden`
- `userScore`
- `categories`
- `tags`
- `features`
- `genres`
- `developers`
- `publishers`
- `series`

## config.yaml 的含义

### 连接配置

- `base_url`: Playnite Bridge API 地址（例如 `http://localhost:19821`）
- `token`: Playnite Bridge API Token
- `grist_base_url`: Grist API 地址（例如 `https://<host>/api`）
- `grist_doc_id`: Grist 文档 ID
- `grist_api_key`: Grist API Key
- `grist_table_name`: 目标表名

### P2G 配置

- `limit`: Playnite 分页大小
- `max_pages`: Playnite 最大分页数
- `include_hidden`: 是否包含隐藏游戏
- `grist_batch_size`: Grist 批处理大小
- `grist_register_choices`: 是否自动补齐 ChoiceList 选项（默认 `true`）
- `grist_delete_missing`: 是否删除 Playnite 不再存在的 Grist 记录（默认 `true`）
- `detail_sync_enabled`: 是否请求详情接口（默认 `true`）
- `detail_full_backfill`: 是否全量详情回填（默认 `false`）
- `sync_state_path`: P2G 状态文件路径（默认 `sync_state.json`）

### G2P 配置

- `g2p_fields`: 回写白名单字段（逗号分隔）
- `g2p_apply`: 默认写回模式（可被命令行参数覆盖）
- `g2p_state_path`: G2P 状态文件路径（默认 `sync_state_g2p.json`）
- `g2p_max_pages`: Grist 最大分页数（默认 `2000`）
- `g2p_incremental_cutoff`: 是否启用增量截断（默认 `true`）
- `g2p_allow_when_playnite_modified_missing`: Playnite 缺少 modified 时是否允许继续判定（默认 `true`）
- `g2p_edited_after_sync_grace_seconds`: `editedAt` 相对 `syncedAt` 的生效秒差（默认 `300`）

### 运行示例

Playnite to Grist：

```bash
python sync_playnite_to_grist.py
```

Grist to Playnite dry-run：

```bash
python sync_grist_to_playnite.py --dry-run
```

Grist to Playnite apply：

```bash
python sync_grist_to_playnite.py --apply
```

双向同步：

```bash
python run_sync_job.py --g2p-dry-run
```
