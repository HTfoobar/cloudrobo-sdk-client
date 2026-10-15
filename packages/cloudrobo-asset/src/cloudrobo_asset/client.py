import logging
import os
import time
from typing import Any, Dict, List, Optional, Set

from cloudrobo_core.sdk import BaseClient
from cloudrobo_core.sdk.obs_client import OBSClient

from .validators import AssetValidator, ValidationError, validate_params
from .validators.rules import EXT_METADATA_RULES, TAG_PATTERN

logger = logging.getLogger(__name__)


class AssetClient(BaseClient):
    SERVICE = "cloudrobo-asset-manager"

    # 创建新版本时后端要求 ext_metadata 必填的类型（与 EXT_METADATA_RULES 对齐）
    EXT_METADATA_REQUIRED_TYPES = set(EXT_METADATA_RULES.keys())

    def __init__(self, http_client):
        super().__init__(http_client)
        self._tag_cache: Dict[str, tuple] = {}
        self._tag_cache_ttl: float = 3600.0

    def list_repositories(self, **params) -> Dict:
        return self._client.get(self._url("/v1/repositories"), params=params)

    def list_catalogs(self, repository_id: str, **params) -> Dict:
        params["repository_id"] = repository_id
        return self._client.get(self._url("/v1/catalogs"), params=params)

    def show_catalog(self, catalog_id: str) -> Dict:
        return self._client.get(self._url(f"/v1/catalogs/{catalog_id}"))

    @validate_params("create_asset")
    def create_asset(self, req: Dict) -> Dict:
        if req.get("tags"):
            req["tags"] = self._validate_tags(req["tags"], req.get("type"), req.get("sub_type"))
        if req.get("type") == "algorithm":
            self._validate_algorithm_image_tags(req)
        return self._client.post(self._url("/v1/assets"), json=req)

    def list_assets(self, **params) -> Dict:
        return self._client.get(self._url("/v1/assets"), params=params)

    @validate_params("update_asset")
    def update_asset(self, asset_id: str, req: Dict) -> Dict:
        if "tags" in req and req["tags"] is not None:
            existing = self.show_asset(asset_id)
            asset_type = existing.get("type") if isinstance(existing, dict) else None
            if asset_type in ("algorithm", "image") and (
                not isinstance(req["tags"], list) or len(req["tags"]) == 0
            ):
                raise ValidationError([
                    f"tags cannot be empty for type '{asset_type}': "
                    f"algorithm and image must have at least one task scenario tag"
                ])
            if req["tags"]:
                sub_type = existing.get("sub_type") if isinstance(existing, dict) else None
                req["tags"] = self._validate_tags(req["tags"], asset_type, sub_type)
            if asset_type == "algorithm":
                self._validate_algorithm_image_tags({**req, "type": "algorithm"})
        return self._client.put(self._url(f"/v1/assets/{asset_id}"), json=req)

    def delete_asset(self, asset_id: str) -> Any:
        return self._client.delete(self._url(f"/v1/assets/{asset_id}"))

    def show_asset(self, asset_id: str) -> Dict:
        return self._client.get(self._url(f"/v1/assets/{asset_id}"))

    def batch_delete_assets(self, req: Dict) -> Dict:
        return self._client.post(self._url("/v1/assets/batch-delete"), json=req)

    @validate_params("create_version")
    def create_asset_version(self, asset_id: str, req: Dict) -> Dict:
        return self._client.post(self._url(f"/v1/assets/{asset_id}/versions"), json=req)

    def list_asset_versions(self, asset_id: str, **params) -> Dict:
        return self._client.get(self._url(f"/v1/assets/{asset_id}/versions"), params=params)

    def show_asset_version(self, asset_id: str, version_id: str) -> Dict:
        return self._client.get(self._url(f"/v1/assets/{asset_id}/versions/{version_id}"))

    @validate_params("update_version")
    def update_asset_version(self, asset_id: str, version_id: str, req: Dict) -> Dict:
        return self._client.put(self._url(f"/v1/assets/{asset_id}/versions/{version_id}"), json=req)

    def delete_asset_version(self, asset_id: str, version_id: str) -> Any:
        return self._client.delete(self._url(f"/v1/assets/{asset_id}/versions/{version_id}"))

    def batch_delete_asset_versions(self, asset_id: str, req: Dict) -> Dict:
        return self._client.post(self._url(f"/v1/assets/{asset_id}/versions/batch-delete"), json=req)

    def add_tags(self, asset_id: str, tags: List[str]) -> Dict:
        tags = self._validate_tags(tags)
        return self._client.post(self._url(f"/v1/assets/{asset_id}/tags"), json={"tags": tags})

    def delete_tag(self, asset_id: str, tag: str) -> Any:
        return self._client.delete(self._url(f"/v1/assets/{asset_id}/tags/{tag}"))

    def show_asset_tree(self, asset_id: str, version_id: str, query_type: str) -> Dict:
        return self._client.get(self._url(f"/v1/assets/{asset_id}/versions/{version_id}/tree"), params={"type": query_type})

    def check_asset_permission(self, asset_id: str, version_id: str, req: Dict) -> Dict:
        return self._client.post(self._url(f"/v1/assets/{asset_id}/versions/{version_id}/check-permission"), json=req)

    def list_asset_actions(self, asset_id: str, version_id: str) -> Dict:
        return self._client.get(self._url(f"/v1/assets/{asset_id}/versions/{version_id}/actions"))

    def create_asset_action(self, asset_id: str, version_id: str, req: Dict) -> Dict:
        """Create one or more actions on an asset version.

        req accepts two shapes:
        - Single action dict: {"action": "FFT", "algorithm": {...}, ...}
        - Batch wrapper: {"actions": [{...}, {...}]}

        A single action dict is auto-wrapped into {"actions": [req]} before posting.
        """
        body = req if "actions" in req else {"actions": [req]}
        return self._client.post(self._url(f"/v1/assets/{asset_id}/versions/{version_id}/actions"), json=body)

    def show_asset_action(self, asset_id: str, version_id: str, action: str) -> Dict:
        return self._client.get(self._url(f"/v1/assets/{asset_id}/versions/{version_id}/actions/{action}"))

    def update_asset_action(self, asset_id: str, version_id: str, action: str, req: Dict) -> Dict:
        return self._client.put(self._url(f"/v1/assets/{asset_id}/versions/{version_id}/actions/{action}"), json=req)

    def delete_asset_action(self, asset_id: str, version_id: str, action: str) -> Any:
        return self._client.delete(self._url(f"/v1/assets/{asset_id}/versions/{version_id}/actions/{action}"))

    def search_assets(self, req: Dict) -> Dict:
        return self._client.post(self._url("/v1/asset-service/search"), json=req)

    def list_publication_assets(self, **params) -> Dict:
        return self._client.get(self._url("/v1/asset-service/publication-assets"), params=params)

    def list_all_tags(self, **params) -> Dict:
        return self._client.get(self._url("/v1/asset-tags"), params=params)

    def _resolve_version(self, versions: Dict, version_id: str = None) -> Optional[Dict]:
        """Resolve version data from versions list by version_id."""
        data = versions.get("data", [])
        if not version_id:
            return data[0] if data else None
        for v in data:
            if v.get("id") == version_id or v.get("version_id") == version_id:
                return v
        return None

    def _get_all_valid_tags(self, asset_type: str = None, sub_type: str = None) -> Set[str]:
        cache_key = f"{asset_type or ''}:{sub_type or ''}"
        now = time.time()
        cached = self._tag_cache.get(cache_key)
        if cached is not None:
            tags, ts = cached
            if now - ts < self._tag_cache_ttl:
                return tags

        valid_tags = set()
        for lang in ("zh", "en"):
            params = {"language": lang}
            if asset_type:
                params["type"] = asset_type
            if sub_type:
                params["sub_type"] = sub_type
            result = self.list_all_tags(**params)
            if isinstance(result, dict) and "data" in result:
                for item in result["data"]:
                    tags_data = item.get("tags", {})
                    if isinstance(tags_data, dict) and "data" in tags_data:
                        for tag_item in tags_data["data"]:
                            if "tag" in tag_item:
                                valid_tags.add(tag_item["tag"])

        self._tag_cache[cache_key] = (valid_tags, now)
        return valid_tags

    def _validate_tags(self, tags: List[str], asset_type: str = None, sub_type: str = None) -> List[str]:
        if not tags:
            return []
        format_valid = [tag for tag in tags if isinstance(tag, str) and tag and TAG_PATTERN.match(tag)]
        format_invalid = [tag for tag in tags if isinstance(tag, str) and tag and not TAG_PATTERN.match(tag)]
        non_string = [tag for tag in tags if not isinstance(tag, str) or not tag]
        if non_string:
            logger.warning("Non-string or empty tags dropped: %s", non_string)
        if format_invalid:
            logger.warning("Tags with invalid format dropped: %s", format_invalid)
        valid_tags = self._get_all_valid_tags(asset_type, sub_type)
        validated = [tag for tag in format_valid if tag in valid_tags]
        not_in_server = [tag for tag in format_valid if tag not in valid_tags]
        if not_in_server:
            logger.warning("Tags not in server predefined list dropped: %s", not_in_server)
        return list(dict.fromkeys(validated))

    def _validate_algorithm_image_tags(self, req: Dict) -> None:
        """校验算法引用的镜像资产的 tags 是镜像 tags 的子集。

        后端规则：创建算法时，算法的任务标签必须是使用的镜像的任务标签的子集。
        算法通过 ext_metadata.engine.asset_id 引用镜像资产。
        """
        ext_metadata = req.get("ext_metadata")
        if not isinstance(ext_metadata, dict):
            return
        engine = ext_metadata.get("engine")
        if not isinstance(engine, dict):
            return
        image_asset_id = engine.get("asset_id")
        if not image_asset_id or not isinstance(image_asset_id, str):
            return

        algo_tags = set(req.get("tags") or [])
        if not algo_tags:
            return

        try:
            image_asset = self.show_asset(image_asset_id)
        except Exception as e:
            logger.warning(
                "Failed to fetch referenced image asset %s for tags comparison: %s",
                image_asset_id, e,
            )
            return

        if not isinstance(image_asset, dict):
            return
        image_tags = set(image_asset.get("tags") or [])
        if not image_tags:
            return

        if not algo_tags.issubset(image_tags):
            extra = sorted(algo_tags - image_tags)
            raise ValidationError([
                f"tags mismatch: algorithm tags {sorted(algo_tags)} must be a subset of "
                f"the referenced image asset tags {sorted(image_tags)} "
                f"(extra tags not in image: {extra}, image asset_id: {image_asset_id})"
            ])

    def _validate_local_path(self, local_path: str):
        """Validate local path exists and is a directory."""
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local path does not exist: {local_path}")
        if os.path.isfile(local_path):
            raise ValueError(f"Local path must be a directory, not a file: {local_path}")

    def _get_existing_asset(self, asset_id: str) -> Dict:
        """Get existing asset and validate it exists. Returns asset data dict."""
        existing = self.show_asset(asset_id)
        if not existing or (isinstance(existing, dict) and not existing.get("id") and not existing.get("asset_id")):
            raise ValueError(f"Asset with id {asset_id} does not exist")
        return existing

    def _parse_obs_url(self, obs_url: str) -> tuple:
        """Parse obs://bucket/prefix into (bucket_name, obs_prefix). Raises RuntimeError if invalid."""
        if not obs_url or not obs_url.startswith("obs://"):
            raise RuntimeError(f"Invalid OBS URL: {obs_url}")
        bucket_name = obs_url.replace("obs://", "").split("/")[0]
        prefix_start = len(f"obs://{bucket_name}/")
        obs_prefix = obs_url[prefix_start:] if len(obs_url) > prefix_start else ""
        if not obs_prefix:
            raise RuntimeError(f"OBS URL has no path prefix (bucket-only URL): {obs_url}")
        return bucket_name, obs_prefix

    def _upload_folder_to_obs(self, local_path: str, bucket_name: str, obs_prefix: str,
                               part_size: int = 9437184, enable_checkpoint: bool = True):
        """Upload folder to OBS. Raises RuntimeError on failure."""
        obs_client = OBSClient(self._client.config, bucket_name)
        success = obs_client.upload_folder(
            local_path, obs_prefix, show_progress=True,
            enable_checkpoint=enable_checkpoint, part_size=part_size
        )
        if not success:
            raise RuntimeError(f"Failed to upload folder to OBS: obs://{bucket_name}/{obs_prefix}")

    def _update_code_dir_dual_write(self, asset_id: str, version_id: str, orig_ext: Dict, code_dir: str):
        """Update code_dir in ext_metadata for both asset and version (dual-write pattern)."""
        updated_ext = dict(orig_ext or {})
        updated_ext.pop("boot_file", None)
        updated_ext["code_dir"] = code_dir
        self.update_asset_version(asset_id, version_id, {"ext_metadata": updated_ext})
        self.update_asset(asset_id, {"ext_metadata": updated_ext})

    def _build_version_req(self, description: str = None, ext_metadata: Dict = None,
                           version: str = None, status: str = None,
                           parent_asset_version_id: str = None, generation_method: str = None) -> Dict:
        """Build version request dict from optional parameters."""
        version_req = {}
        if description is not None:
            version_req["description"] = description
        if ext_metadata is not None:
            version_req["ext_metadata"] = ext_metadata
        if version is not None:
            version_req["version"] = version
        if status is not None:
            version_req["status"] = status
        if parent_asset_version_id is not None:
            version_req["parent_asset_version_id"] = parent_asset_version_id
        if generation_method is not None:
            version_req["generation_method"] = generation_method
        return version_req

    def _import_algorithm_with_code_upload(self, catalog_url: str, asset_id: str, version_id: str,
                                            local_path: str, orig_ext: Dict,
                                            part_size: int, enable_checkpoint: bool) -> Dict:
        """Algorithm import core flow: upload code -> update code_dir (dual-write asset + version)."""
        upload_prefix = f"{catalog_url.rstrip('/')}/{asset_id}/{version_id}/"
        bucket_name, obs_prefix = self._parse_obs_url(upload_prefix)
        self._upload_folder_to_obs(local_path, bucket_name, obs_prefix, part_size, enable_checkpoint)
        self._update_code_dir_dual_write(asset_id, version_id, orig_ext, upload_prefix)
        return self.show_asset_version(asset_id, version_id)

    def _transition_creating_to_draft(self, asset_id: str, version_id: str, version_data: Dict) -> Dict:
        """If version status is CREATING, transition to DRAFT after successful upload."""
        if version_data.get("status") == "CREATING":
            try:
                self.update_asset_version(asset_id, version_id, {"status": "DRAFT"})
                version_data = self.show_asset_version(asset_id, version_id)
            except Exception as e:
                logger.warning("Failed to update version status CREATING→DRAFT: %s", e)
        return version_data

    def _import_mode1_create_new_asset(self, catalog_id: str, asset_type: str, local_path: str,
                                        name: str, sub_type: str, ext_metadata: Dict,
                                        description: str, status: str, tags: list,
                                        parent_asset_version_id: str, generation_method: str,
                                        part_size: int, enable_checkpoint: bool) -> Dict:
        """Mode 1: Create new asset + version, then upload files."""
        req = {"catalog_id": catalog_id, "type": asset_type}
        if name is not None:
            req["name"] = name
        if sub_type is not None and asset_type == "simulation":
            req["sub_type"] = sub_type
        if description is not None:
            req["description"] = description
        if ext_metadata is not None:
            req["ext_metadata"] = ext_metadata
        if status is not None:
            req["status"] = status
        if tags is not None:
            req["tags"] = self._validate_tags(
                tags, asset_type, sub_type if asset_type == "simulation" else None
            )
        if parent_asset_version_id is not None:
            req["parent_asset_version_id"] = parent_asset_version_id
        if generation_method is not None:
            req["generation_method"] = generation_method

        is_algorithm = asset_type == "algorithm"
        catalog_url = None
        orig_ext = None

        if is_algorithm:
            catalog = self.show_catalog(catalog_id)
            catalog_url = catalog.get("url") if isinstance(catalog, dict) else None
            if not catalog_url or not catalog_url.startswith("obs://"):
                raise RuntimeError(f"Invalid catalog url for algorithm code upload: {catalog_url}")
            if isinstance(req.get("ext_metadata"), dict):
                orig_ext = dict(req["ext_metadata"])
                req["ext_metadata"] = {k: v for k, v in orig_ext.items()
                                       if k not in ("code_dir", "boot_file")}
                req["ext_metadata"]["code_dir"] = catalog_url.rstrip("/") + "/pending"

        asset = self.create_asset(req)
        asset_id = asset.get("id") or asset.get("asset_id")
        version_id = asset.get("latest_version_id")
        if not asset_id or not version_id:
            raise RuntimeError(f"Failed to create asset: API returned asset_id={asset_id}, version_id={version_id}")

        if is_algorithm:
            return self._import_algorithm_with_code_upload(
                catalog_url, asset_id, version_id, local_path, orig_ext,
                part_size, enable_checkpoint
            )
        else:
            version_data = self.show_asset_version(asset_id, version_id)
            obs_url = self._extract_obs_url_from_version(version_data)
            bucket_name, obs_prefix = self._parse_obs_url(obs_url)
            self._upload_folder_to_obs(local_path, bucket_name, obs_prefix, part_size, enable_checkpoint)
            version_data = self.show_asset_version(asset_id, version_id)
            return self._transition_creating_to_draft(asset_id, version_id, version_data)

    def _import_mode2_create_new_version(self, asset_id: str, asset_type: str, local_path: str,
                                          ext_metadata: Dict, description: str, version: str,
                                          status: str, parent_asset_version_id: str,
                                          generation_method: str,
                                          part_size: int, enable_checkpoint: bool) -> Dict:
        """Mode 2: Create new version for existing asset, then upload files."""
        existing = self._get_existing_asset(asset_id)
        existing_type = existing.get("type") if isinstance(existing, dict) else None

        if existing_type == "algorithm":
            return self._import_mode2_algorithm(
                asset_id, existing, local_path, ext_metadata, description, version,
                status, parent_asset_version_id, generation_method, part_size, enable_checkpoint
            )
        else:
            return self._import_mode2_non_algorithm(
                asset_id, existing_type, existing, local_path, ext_metadata, description,
                version, status, parent_asset_version_id, generation_method,
                part_size, enable_checkpoint
            )

    def _import_mode2_algorithm(self, asset_id: str, existing: Dict, local_path: str,
                                 ext_metadata: Dict, description: str, version: str,
                                 status: str, parent_asset_version_id: str,
                                 generation_method: str,
                                 part_size: int, enable_checkpoint: bool) -> Dict:
        """Mode 2 algorithm: get catalog from asset, create version, upload code."""
        existing_catalog_id = existing.get("catalog_id") if isinstance(existing, dict) else None
        if not existing_catalog_id:
            raise RuntimeError(f"Failed to get catalog_id from asset {asset_id}")
        catalog = self.show_catalog(existing_catalog_id)
        catalog_url = catalog.get("url") if isinstance(catalog, dict) else None
        if not catalog_url or not catalog_url.startswith("obs://"):
            raise RuntimeError(f"Invalid catalog url for algorithm code upload: {catalog_url}")

        orig_ext = dict(ext_metadata) if isinstance(ext_metadata, dict) else None
        if orig_ext is None:
            raise ValueError(
                "ext_metadata is required to create a new version for algorithm asset. "
                "Pass it via --ext-metadata (must include engine and command)."
            )

        version_ext = {k: v for k, v in orig_ext.items() if k not in ("code_dir", "boot_file")}
        version_ext["code_dir"] = catalog_url.rstrip("/") + "/pending"
        errs = AssetValidator().validate_ext_metadata("algorithm", None, version_ext)
        if errs:
            raise ValidationError(errs)

        version_req = self._build_version_req(description, version_ext, version, status,
                                               parent_asset_version_id, generation_method)
        new_version = self.create_asset_version(asset_id, version_req)
        version_id = new_version.get("id") or new_version.get("version_id")
        if not version_id:
            raise RuntimeError(f"Failed to create new version for asset {asset_id}: API returned no version ID")

        return self._import_algorithm_with_code_upload(
            catalog_url, asset_id, version_id, local_path, orig_ext,
            part_size, enable_checkpoint
        )

    def _import_mode2_non_algorithm(self, asset_id: str, existing_type: str, existing: Dict,
                                     local_path: str, ext_metadata: Dict, description: str,
                                     version: str, status: str, parent_asset_version_id: str,
                                     generation_method: str,
                                     part_size: int, enable_checkpoint: bool) -> Dict:
        """Mode 2 non-algorithm: validate ext_metadata, create version, upload files."""
        if ext_metadata is not None:
            errs = AssetValidator().validate_ext_metadata(
                existing_type, existing.get("sub_type"), ext_metadata)
            if errs:
                raise ValidationError(errs)
        elif existing_type in self.EXT_METADATA_REQUIRED_TYPES:
            raise ValueError(
                f"ext_metadata is required to create a new version for type '{existing_type}' asset. "
                f"Pass it via --ext-metadata (users may modify it per version)."
            )

        version_req = self._build_version_req(description, ext_metadata, version, status,
                                               parent_asset_version_id, generation_method)
        new_version = self.create_asset_version(asset_id, version_req)
        version_id = new_version.get("id") or new_version.get("version_id")
        if not version_id:
            raise RuntimeError(f"Failed to create new version for asset {asset_id}: API returned no version ID")

        version_data = self.show_asset_version(asset_id, version_id)
        obs_url = self._extract_obs_url_from_version(version_data)
        bucket_name, obs_prefix = self._parse_obs_url(obs_url)
        self._upload_folder_to_obs(local_path, bucket_name, obs_prefix, part_size, enable_checkpoint)
        version_data = self.show_asset_version(asset_id, version_id)
        return self._transition_creating_to_draft(asset_id, version_id, version_data)

    def _import_mode3_reuse_version(self, asset_id: str, version_id: str,
                                     local_path: str, part_size: int, enable_checkpoint: bool) -> Dict:
        """Mode 3: Reuse existing version, upload files directly (for retry/incremental upload)."""
        existing = self._get_existing_asset(asset_id)
        existing_type = existing.get("type") if isinstance(existing, dict) else None

        if existing_type == "algorithm":
            raise ValueError(
                "Incremental upload (asset_id + version_id) is not supported for algorithm assets. "
                "Use --asset-id to create a new version, or import without asset-id to create a new asset."
            )
        if existing_type == "image":
            raise ValueError(
                "import_asset does not support image assets. "
                "Image assets are SWR references with no local code upload semantics. "
                "Use create_asset to manage image assets."
            )

        version_data = self.show_asset_version(asset_id, version_id)
        if not version_data or (isinstance(version_data, dict) and not version_data.get("id") and not version_data.get("version_id")):
            raise ValueError(f"Version with id {version_id} does not exist for asset {asset_id}")

        obs_url = self._extract_obs_url_from_version(version_data)
        bucket_name, obs_prefix = self._parse_obs_url(obs_url)
        self._upload_folder_to_obs(local_path, bucket_name, obs_prefix, part_size, enable_checkpoint)
        version_data = self.show_asset_version(asset_id, version_id)
        return self._transition_creating_to_draft(asset_id, version_id, version_data)

    def import_asset(self, catalog_id: str, asset_type: str, local_path: str, name: str = None,
                     sub_type: str = None, ext_metadata: Dict = None, asset_id: str = None,
                     version_id: str = None,
                     description: str = None, part_size: int = 9437184, enable_checkpoint: bool = True,
                     version: str = None, status: str = None, tags: list = None,
                     parent_asset_version_id: str = None, generation_method: str = None,
                     ) -> Dict:
        """Import asset: register asset (auto-creates version) and upload local folder to OBS.

        Three modes:
        - Mode 1 (no asset_id): Create new asset + version, then upload
        - Mode 2 (asset_id only): Create new version for existing asset, then upload
        - Mode 3 (asset_id + version_id): Reuse existing version, upload directly

        Args:
            catalog_id: Catalog ID (required for Mode 1)
            name: Asset name (required for Mode 1)
            asset_type: Asset type
            local_path: Local folder path to upload
            sub_type: Asset sub_type, optional
            ext_metadata: Extended metadata dict, optional
            asset_id: Asset ID (for Mode 2/3)
            version_id: Version ID (for Mode 3)
            description: Asset description, optional
            enable_checkpoint: Enable resumable upload, default True
            part_size: Part size in bytes for OBS upload, default 9MB
            version: Version string, optional (for create version)
            status: Asset status, optional
            tags: Tags list, optional (for create asset)
            parent_asset_version_id: Parent asset version ID, optional
            generation_method: Generation method, optional

        Returns:
            Version data dict from show_asset_version (contains version details from backend)
        """
        self._validate_local_path(local_path)

        if asset_id and version_id:
            return self._import_mode3_reuse_version(
                asset_id, version_id, local_path, part_size, enable_checkpoint
            )
        elif asset_id:
            return self._import_mode2_create_new_version(
                asset_id, asset_type, local_path, ext_metadata, description,
                version, status, parent_asset_version_id, generation_method,
                part_size, enable_checkpoint
            )
        else:
            return self._import_mode1_create_new_asset(
                catalog_id, asset_type, local_path, name, sub_type, ext_metadata,
                description, status, tags, parent_asset_version_id, generation_method,
                part_size, enable_checkpoint
            )

    def export_asset(self, asset_id: str, local_path: str, version_id: str = None,
                     enable_checkpoint: bool = True, part_size: int = 5242880) -> Dict:
        """Export asset: get OBS URL from latest version, download to local path.

        Args:
            asset_id: Asset ID
            local_path: Local target path
            version_id: Specific version to export, uses latest if not specified
            enable_checkpoint: Enable resumable download, default True
            part_size: Part size in bytes for OBS download, default 5MB

        Returns:
            Export result dict with asset_id, version_id, obs_url, local_path, status
        """
        asset_data = self.show_asset(asset_id)
        asset_type = asset_data.get("type")

        if asset_type == "image":
            raise ValueError(
                "export-asset does not support image assets. "
                "Image assets are SWR references with no downloadable files. "
                "Use cloudrobo asset create-asset to manage image assets."
            )

        versions = self.list_asset_versions(asset_id)
        if not versions or (isinstance(versions, dict) and not versions.get("data")):
            raise RuntimeError(f"No versions found for asset {asset_id}")

        if not version_id:
            version_data = self._resolve_version(versions)
        else:
            version_data = self._resolve_version(versions, version_id)
        if not version_data:
            raise RuntimeError(f"Version {version_id} not found for asset {asset_id}")

        resolved_version_id = version_data.get("id") or version_data.get("version_id")

        if asset_type == "algorithm":
            version_detail = self.show_asset_version(asset_id, resolved_version_id)
            ext = version_detail.get("ext_metadata") or {}
            obs_url = ext.get("code_dir")
            if not obs_url or not obs_url.startswith("obs://"):
                raise RuntimeError(
                    f"Algorithm asset {asset_id} version {resolved_version_id} has no code_dir in ext_metadata. "
                    f"This usually means the algorithm was created but code upload failed or was skipped. "
                    f"Use import-asset with --asset-id and --version-id to upload code first."
                )
        else:
            obs_url = self._extract_obs_url_from_version(version_data)
            version_detail = self.show_asset_version(asset_id, resolved_version_id)
            if not obs_url or not obs_url.startswith("obs://"):
                raise RuntimeError(f"Invalid OBS URL for asset {asset_id} version {resolved_version_id}: {obs_url}")

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local path does not exist: {local_path}")
        if os.path.isfile(local_path):
            raise ValueError(f"Local path must be a directory, not a file: {local_path}")

        export_path = os.path.join(local_path, asset_id)
        os.makedirs(export_path, exist_ok=True)

        bucket_name, obs_prefix = self._parse_obs_url(obs_url)
        obs_client = OBSClient(self._client.config, bucket_name)
        success = obs_client.download_folder(obs_prefix, export_path, show_progress=True,
                                              enable_checkpoint=enable_checkpoint, part_size=part_size)
        if not success:
            raise RuntimeError(f"Failed to download folder from OBS: {obs_url}")

        metadata = self._build_export_metadata(asset_data, version_detail)
        readme_path = self._write_readme(export_path, metadata)

        return {
            "asset_id": asset_id,
            "version_id": resolved_version_id,
            "obs_url": obs_url,
            "local_path": export_path,
            "readme_path": readme_path,
            "metadata": metadata,
            "status": "exported",
        }

    @staticmethod
    def _extract_obs_url_from_version(version_data: Dict) -> Optional[str]:
        """Extract OBS URL from a single version data dict, supporting nested keys."""
        for key in ("url", "obs_url", "file_url", "path", "obs_path"):
            value = version_data.get(key) or ""
            if value:
                return value
        for nested_key in ("storage", "file_info", "location"):
            nested = version_data.get(nested_key)
            if isinstance(nested, dict):
                for key in ("url", "obs_url", "path", "obs_path"):
                    value = nested.get(key) or ""
                    if value:
                        return value
        return None

    @staticmethod
    def _build_export_metadata(asset_data: Dict, version_data: Dict) -> Dict:
        """从资产和版本详情构建 frontmatter 字典（不包含 asset_id 和 catalog_id）。"""
        fm = {
            "name": asset_data.get("name"),
            "type": asset_data.get("type"),
        }
        if asset_data.get("tags"):
            fm["tags"] = asset_data["tags"]
        if asset_data.get("sub_type"):
            fm["sub_type"] = asset_data["sub_type"]
        fm["description"] = version_data.get("description") or asset_data.get("description")
        fm["status"] = version_data.get("status") or asset_data.get("status")
        ext_metadata = version_data.get("ext_metadata") or asset_data.get("ext_metadata")
        if ext_metadata:
            fm["ext_metadata"] = ext_metadata
        fm["version"] = version_data.get("version")
        if version_data.get("parent_asset_version_id"):
            fm["parent_asset_version_id"] = version_data["parent_asset_version_id"]
        if version_data.get("generation_method"):
            fm["generation_method"] = version_data["generation_method"]
        return {k: v for k, v in fm.items() if v not in (None, [], {}, "")}

    @staticmethod
    def _write_readme(local_path: str, metadata: Dict) -> str:
        """写入 README.md (frontmatter + body)，返回文件路径。

        如果 README.md 已存在，保留原有 body 部分，只替换 frontmatter。
        如果不存在，使用默认模板生成。
        """
        import yaml as _yaml

        readme_path = os.path.join(local_path, "README.md")
        fm_str = _yaml.dump(metadata, default_flow_style=False, allow_unicode=True, sort_keys=False)
        new_frontmatter = f"---\n{fm_str}---\n"

        if os.path.isfile(readme_path):
            with open(readme_path, "r", encoding="utf-8") as f:
                existing_content = f.read()
            body = AssetClient._extract_body_from_readme(existing_content)
            content = new_frontmatter + "\n" + body
        else:
            name = metadata.get("name", "Asset")
            desc = metadata.get("description", "")
            folder_name = os.path.basename(os.path.abspath(local_path))
            body_lines = [f"# {name}", ""]
            if desc:
                body_lines.extend([desc, ""])
            body_lines.extend([
                "## Import",
                "",
                "```bash",
                f"cloudrobo asset import-asset --catalog-id <your-catalog-id> --local-path ./{folder_name}",
                "```",
                "",
            ])
            content = new_frontmatter + "\n" + "\n".join(body_lines)

        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(content)
        return readme_path

    @staticmethod
    def _extract_body_from_readme(content: str) -> str:
        """从 README.md 内容中提取 body 部分（frontmatter 之后的内容）。

        如果没有 frontmatter，返回全部内容。
        """
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                return parts[2].lstrip("\n")
        return content
