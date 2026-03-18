import requests
import json
import time
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys

@dataclass
class AuthFile:
    """认证文件数据结构"""
    name: str
    authIndex: str = ""
    status: str = ""
    statusMessage: str = ""
    disabled: bool = False
    unavailable: bool = False
    channel: str = ""
    chatgptAccountId: str = ""
    usedPercent: Optional[float] = None
    resetText: str = "-"
    lastStatusCode: Optional[int] = None
    queryState: str = "unqueried"  # unqueried, ok, quota, failed, unknown
    hasQuota: Optional[bool] = None
    deleteEligible: bool = False


class CPAuthCleaner:
    """CPA认证文件清理器 - 专用于清理401失效凭证"""
    
    def __init__(self, base_url: str, token: str, concurrency: int = 4):
        """
        初始化清理器
        
        Args:
            base_url: 基础URL，例如 https://your-domain.com
            token: 认证令牌
            concurrency: 并发数
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.concurrency = concurrency
        
        # API端点
        self.auth_files_url = f"{self.base_url}/v0/management/auth-files"
        self.auth_files_status_url = f"{self.base_url}/v0/management/auth-files/status"
        self.api_call_url = f"{self.base_url}/v0/management/api-call"
        
        # 常量
        self.usage_url = "https://chatgpt.com/backend-api/wham/usage"
        self.default_ua = "codex_cli_rs/universal (Windows)"
        
        # 会话
        self.session = requests.Session()
        self.session.headers.update(self._get_headers())
    
    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9",
            "referer": f"{self.base_url}/management.html",
            "authorization": f"Bearer {self.token}"
        }
        return headers
    
    def _safe_json(self, text: str) -> Optional[Dict]:
        """安全解析JSON"""
        try:
            return json.loads(text)
        except:
            return None
    
    def _extract_file_name(self, item: Dict) -> str:
        """提取文件名"""
        for key in ["name", "id", "filename", "file_name"]:
            v = item.get(key)
            if v:
                return str(v)
        return "(no-name)"
    
    def _extract_auth_index(self, item: Dict) -> str:
        """提取authIndex"""
        for key in ["authIndex", "auth_index", "authindex"]:
            v = item.get(key)
            if v:
                return str(v)
        return ""
    
    def _extract_chatgpt_account_id(self, item: Dict) -> str:
        """提取ChatGPT账号ID"""
        nested = item.get("id_token", {}).get("chatgpt_account_id") or \
                 item.get("idToken", {}).get("chatgpt_account_id")
        if nested:
            return str(nested)
        
        for key in ["chatgpt_account_id", "chatgptAccountId", "account_id", "accountId"]:
            v = item.get(key)
            if v:
                return str(v)
        return ""
    
    def _extract_channel(self, item: Dict) -> str:
        """提取渠道信息"""
        # 直接字段
        for key in ["channel", "provider", "type", "model_provider", "modelProvider"]:
            value = item.get(key, "")
            if value:
                return str(value).strip().lower()
        
        # 嵌套字段
        nested_values = [
            item.get("id_token", {}).get("provider"),
            item.get("idToken", {}).get("provider"),
            item.get("id_token", {}).get("channel"),
            item.get("idToken", {}).get("channel"),
        ]
        for value in nested_values:
            if value:
                return str(value).strip().lower()
        
        return "unknown"
    
    def _is_codex_channel(self, item: AuthFile) -> bool:
        """判断是否为Codex渠道"""
        return item.channel.lower() == "codex"
    
    def _supports_active_check(self, item: AuthFile) -> bool:
        """是否支持活跃检查"""
        return self._is_codex_channel(item)
    
    def _normalize_files_payload(self, data: Any) -> List[AuthFile]:
        """规范化文件数据"""
        files = []
        
        if isinstance(data, dict) and "files" in data and isinstance(data["files"], list):
            raw_files = data["files"]
        elif isinstance(data, list):
            raw_files = data
        else:
            raw_files = []
        
        for item in raw_files:
            if not isinstance(item, dict):
                continue
            
            auth_file = AuthFile(
                name=self._extract_file_name(item),
                authIndex=self._extract_auth_index(item),
                status=str(item.get("status", "")),
                statusMessage=str(item.get("status_message", item.get("statusMessage", ""))),
                disabled=bool(item.get("disabled", False)),
                unavailable=bool(item.get("unavailable", False)),
                channel=self._extract_channel(item),
                chatgptAccountId=self._extract_chatgpt_account_id(item)
            )
            files.append(auth_file)
        
        return files
    
    def fetch_all_files(self) -> List[AuthFile]:
        """获取所有文件"""
        try:
            resp = self.session.get(self.auth_files_url)
            resp.raise_for_status()
            data = resp.json()
            return self._normalize_files_payload(data)
        except requests.exceptions.RequestException as e:
            print(f"获取文件列表失败: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"状态码: {e.response.status_code}")
                print(f"响应: {e.response.text}")
            raise
    
    def fetch_non_active_files(self) -> List[AuthFile]:
        """获取非活跃文件"""
        all_files = self.fetch_all_files()
        return [f for f in all_files if f.status.lower() != "active"]
    
    def query_usage_by_auth_index(self, file_item: AuthFile) -> Tuple[bool, Optional[int], Optional[Dict], str]:
        """查询使用情况"""
        if not file_item.authIndex:
            return False, None, None, "missing authIndex"
        
        headers = {
            "Authorization": "Bearer $TOKEN$",
            "Content-Type": "application/json",
            "User-Agent": self.default_ua
        }
        if file_item.chatgptAccountId:
            headers["Chatgpt-Account-Id"] = file_item.chatgptAccountId
        
        body = {
            "authIndex": file_item.authIndex,
            "method": "GET",
            "url": self.usage_url,
            "header": headers
        }
        
        try:
            resp = self.session.post(
                self.api_call_url,
                json=body
            )
            
            data = self._safe_json(resp.text)
            status_code = data.get("status_code") if data else None
            
            body_obj = None
            body_text = ""
            
            if data and isinstance(data.get("body"), str):
                body_text = data["body"]
                body_obj = self._safe_json(data["body"])
            elif data and isinstance(data.get("body"), dict):
                body_obj = data["body"]
                body_text = json.dumps(data["body"], ensure_ascii=False)
            else:
                body_text = resp.text
            
            ok = resp.status_code == 200 and status_code == 200
            return ok, status_code, body_obj, body_text
            
        except requests.exceptions.RequestException as e:
            print(f"查询失败: {e}")
            return False, None, None, str(e)
    
    def _normalize_used_percent(self, value: Any) -> Optional[float]:
        """规范化使用百分比"""
        try:
            num = float(value)
            return max(0, min(100, num)) if num is not None else None
        except (TypeError, ValueError):
            return None
    
    def _format_usage_reset_text(self, window_info: Optional[Dict]) -> str:
        """格式化重置时间文本"""
        if not window_info:
            return "-"
        
        reset_at = window_info.get("reset_at")
        if reset_at is not None:
            try:
                ts = int(reset_at)
                return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
            except (ValueError, TypeError):
                pass
        
        reset_after = window_info.get("reset_after_seconds")
        if reset_after is not None:
            try:
                sec = int(reset_after)
                return f"{sec}s"
            except (ValueError, TypeError):
                pass
        
        return "-"
    
    def _parse_usage_snapshot(self, body_obj: Optional[Dict]) -> Dict:
        """解析使用情况快照"""
        if not body_obj:
            return {
                "usedPercent": None,
                "resetText": "-",
                "hasQuota": None,
                "limitReached": False
            }
        
        rate_limit = body_obj.get("rate_limit", {})
        windows = []
        
        primary = rate_limit.get("primary_window")
        if primary and isinstance(primary, dict):
            windows.append({"label": "5h", "data": primary})
        
        secondary = rate_limit.get("secondary_window")
        if secondary and isinstance(secondary, dict):
            windows.append({"label": "7d", "data": secondary})
        
        if not windows:
            return {
                "usedPercent": None,
                "resetText": "-",
                "hasQuota": None,
                "limitReached": False
            }
        
        display_window = windows[0]
        max_used_percent = None
        
        for window in windows:
            used_percent = self._normalize_used_percent(window["data"].get("used_percent"))
            if used_percent is not None:
                if max_used_percent is None or used_percent > max_used_percent:
                    max_used_percent = used_percent
                    display_window = window
        
        limit_reached = (
            rate_limit.get("limit_reached") == True or
            rate_limit.get("allowed") == False or
            any(self._normalize_used_percent(w["data"].get("used_percent")) == 100 for w in windows)
        )
        
        return {
            "usedPercent": max_used_percent,
            "resetText": self._format_usage_reset_text(display_window["data"]),
            "hasQuota": not limit_reached,
            "limitReached": limit_reached
        }
    
    def _is_quota_result(self, status_code: Optional[int], body_obj: Optional[Dict], body_text: str) -> bool:
        """判断是否为额度问题"""
        code = int(status_code) if status_code is not None else 0
        
        # 检查body_obj
        if body_obj:
            if body_obj.get("rate_limit", {}).get("limit_reached") == True:
                return True
            if body_obj.get("rate_limit", {}).get("allowed") == False:
                return True
        
        # 检查文本
        text = f"{json.dumps(body_obj or {})} {body_text}".lower()
        if "quota exhausted" in text:
            return True
        if "limit reached" in text:
            return True
        if "payment_required" in text:
            return True
        
        return code == 402
    
    def _mark_query_result(self, item: AuthFile, result: Tuple, usage_snapshot: Dict):
        """标记查询结果"""
        ok, status_code, body_obj, body_text = result
        
        item.lastStatusCode = status_code
        item.usedPercent = usage_snapshot.get("usedPercent")
        item.resetText = usage_snapshot.get("resetText", "-")
        item.hasQuota = usage_snapshot.get("hasQuota")
        
        if status_code == 200:
            item.queryState = "quota" if usage_snapshot.get("hasQuota") == False else "ok"
            item.deleteEligible = False
        elif status_code == 401:
            item.queryState = "failed"
            item.deleteEligible = True
            item.hasQuota = None
        elif self._is_quota_result(status_code, body_obj, body_text):
            item.queryState = "quota"
            item.deleteEligible = False
            item.hasQuota = False
        else:
            item.queryState = "unknown"
            item.deleteEligible = False
            item.hasQuota = None
    
    def _mark_query_failed(self, item: AuthFile, status_code: Any = "ERR"):
        """标记查询失败"""
        item.lastStatusCode = status_code
        item.usedPercent = None
        item.resetText = "-"
        item.hasQuota = None
        item.queryState = "unknown"
        item.deleteEligible = False
    
    def query_files_batch(self, files: List[AuthFile]) -> List[AuthFile]:
        """批量查询文件状态"""
        codex_files = [f for f in files if self._supports_active_check(f)]
        
        if not codex_files:
            print("没有Codex文件需要检查")
            return files
        
        print(f"开始检查 {len(codex_files)} 个Codex文件...")
        
        def process_file(file_item: AuthFile) -> AuthFile:
            try:
                ok, status_code, body_obj, body_text = self.query_usage_by_auth_index(file_item)
                if ok or status_code is not None:
                    usage_snapshot = self._parse_usage_snapshot(body_obj)
                    self._mark_query_result(file_item, (ok, status_code, body_obj, body_text), usage_snapshot)
                else:
                    self._mark_query_failed(file_item)
            except Exception as e:
                print(f"处理文件 {file_item.name} 时出错: {e}")
                self._mark_query_failed(file_item)
            return file_item
        
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = {executor.submit(process_file, f): f for f in codex_files}
            for i, future in enumerate(as_completed(futures), 1):
                future.result()  # 等待完成，不收集结果
                if i % 10 == 0 or i == len(codex_files):
                    print(f"进度: {i}/{len(codex_files)}")
        
        # 统计结果
        stats = self.collect_stats(files)
        print(f"检查完成: 健康={stats['healthy']}, 无额度={stats['quota']}, "
              f"失效={stats['failed']}, 异常={stats['unknown']}")
        
        return files
    
    def collect_stats(self, files: List[AuthFile]) -> Dict[str, int]:
        """收集统计信息"""
        stats = {
            "total": len(files),
            "unqueried": 0,
            "healthy": 0,
            "quota": 0,
            "failed": 0,
            "unknown": 0,
            "deletable": 0
        }
        
        for item in files:
            if not self._supports_active_check(item):
                continue
            
            if item.queryState == "ok":
                stats["healthy"] += 1
            elif item.queryState == "quota":
                stats["quota"] += 1
            elif item.queryState == "failed":
                stats["failed"] += 1
            elif item.queryState == "unknown":
                stats["unknown"] += 1
            else:
                stats["unqueried"] += 1
            
            if item.deleteEligible:
                stats["deletable"] += 1
        
        return stats
    
    def delete_by_name(self, name: str) -> bool:
        """根据名称删除文件"""
        url = f"{self.auth_files_url}?name={requests.utils.quote(name)}"
        
        try:
            resp = self.session.delete(url)
            if resp.status_code == 401:
                print(f"删除 {name} 失败: 认证失败")
                return False
            
            if resp.status_code == 200:
                data = self._safe_json(resp.text)
                return data is None or data.get("status") == "ok"
            else:
                print(f"删除 {name} 失败: HTTP {resp.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"删除 {name} 时出错: {e}")
            return False
    
    def patch_auth_file_disabled(self, name: str, disabled: bool) -> bool:
        """更新文件的禁用状态"""
        # 尝试主URL
        try:
            resp = self.session.patch(
                self.auth_files_url,
                json={"name": name, "disabled": disabled}
            )
            
            if resp.status_code == 401:
                print(f"更新 {name} 状态失败: 认证失败")
                return False
            
            if resp.status_code == 200:
                data = self._safe_json(resp.text)
                return data is None or data.get("status") == "ok"
            
        except requests.exceptions.RequestException:
            pass
        
        # 尝试备用URL
        try:
            resp = self.session.patch(
                self.auth_files_status_url,
                json={"name": name, "disabled": disabled}
            )
            
            if resp.status_code == 200:
                data = self._safe_json(resp.text)
                return data is None or data.get("status") == "ok"
            else:
                print(f"更新 {name} 状态失败: HTTP {resp.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"更新 {name} 状态时出错: {e}")
            return False
    
    def _sync_local_disabled_state(self, item: AuthFile, disabled: bool):
        """同步本地禁用状态"""
        item.disabled = disabled
        if disabled:
            item.status = "disabled"
            item.statusMessage = "disabled via management API"
        else:
            item.status = "active"
            item.statusMessage = ""
    
    def _update_items_disabled_state(self, items: List[AuthFile], disabled: bool, phase_name: str) -> Tuple[int, int, bool]:
        """批量更新禁用状态"""
        if not items:
            return 0, 0, False
        
        success = 0
        failed = 0
        unsupported = False
        
        for i, item in enumerate(items, 1):
            print(f"{phase_name} {i}/{len(items)}: {item.name}")
            try:
                ok = self.patch_auth_file_disabled(item.name, disabled)
                if ok:
                    self._sync_local_disabled_state(item, disabled)
                    success += 1
                else:
                    failed += 1
            except Exception as e:
                if "404" in str(e) or "405" in str(e) or "501" in str(e):
                    unsupported = True
                    break
                failed += 1
        
        return success, failed, unsupported
    
    def clean_401_files(self, files: List[AuthFile]) -> Dict[str, Any]:
        """
        清理401失效文件的核心方法
        
        执行:
        1. 删除返回401的失效文件
        2. 启用健康的文件
        3. 禁用无额度的文件
        """
        if not files:
            print("没有文件需要处理")
            return {"deleted": 0, "enabled": 0, "disabled": 0}
        
        # 筛选需要操作的文件
        deletable = [
            f for f in files 
            if self._supports_active_check(f) and f.deleteEligible 
            and f.name and f.name != "(no-name)"
        ]
        
        enable_targets = [
            f for f in files
            if self._supports_active_check(f) and f.queryState == "ok" 
            and f.disabled and f.name and f.name != "(no-name)"
        ]
        
        disable_targets = [
            f for f in files
            if self._supports_active_check(f) and f.queryState == "quota" 
            and not f.disabled and f.name and f.name != "(no-name)"
        ]
        
        print(f"待删除(401): {len(deletable)}")
        print(f"待启用(健康): {len(enable_targets)}")
        print(f"待禁用(无额度): {len(disable_targets)}")
        
        if not deletable and not enable_targets and not disable_targets:
            print("无需任何操作")
            return {"deleted": 0, "enabled": 0, "disabled": 0}
        
        results = {"deleted": 0, "enabled": 0, "disabled": 0}
        deleted_set = set()
        
        # 1. 删除401失效文件
        if deletable:
            print(f"\n开始删除 {len(deletable)} 个失效文件...")
            for i, item in enumerate(deletable, 1):
                print(f"删除 {i}/{len(deletable)}: {item.name}")
                try:
                    ok = self.delete_by_name(item.name)
                    if ok:
                        results["deleted"] += 1
                        deleted_set.add(item)
                    time.sleep(0.1)  # 避免请求过快
                except Exception as e:
                    print(f"删除失败: {e}")
        
        # 从列表中移除已删除的文件
        remaining_files = [f for f in files if f not in deleted_set]
        
        # 2. 启用健康的文件
        if enable_targets:
            print(f"\n开始启用 {len(enable_targets)} 个健康文件...")
            success, failed, unsupported = self._update_items_disabled_state(
                enable_targets, False, "启用"
            )
            results["enabled"] = success
            if failed:
                print(f"启用失败: {failed}")
        
        # 3. 禁用无额度的文件
        if disable_targets:
            print(f"\n开始禁用 {len(disable_targets)} 个无额度文件...")
            success, failed, unsupported = self._update_items_disabled_state(
                disable_targets, True, "禁用"
            )
            results["disabled"] = success
            if failed:
                print(f"禁用失败: {failed}")
        
        print(f"\n清理完成: 已删 {results['deleted']}, 已启用 {results['enabled']}, 已禁用 {results['disabled']}")
        return results
    
    def run_full_cleanup(self) -> Dict[str, Any]:
        """
        运行完整清理流程:
        1. 获取所有文件
        2. 批量查询状态
        3. 清理401文件
        """
        print("=" * 50)
        print("开始CPA认证文件清理流程")
        print("=" * 50)
        
        # 1. 获取所有文件
        print("\n[1/3] 获取文件列表...")
        files = self.fetch_all_files()
        print(f"获取到 {len(files)} 个文件")
        
        codex_files = [f for f in files if self._supports_active_check(f)]
        print(f"其中Codex文件: {len(codex_files)}")
        
        if not codex_files:
            print("没有Codex文件，退出")
            return {"deleted": 0, "enabled": 0, "disabled": 0}
        
        # 2. 批量查询状态
        print("\n[2/3] 查询文件状态...")
        files = self.query_files_batch(files)
        
        # 3. 清理401文件
        print("\n[3/3] 执行清理...")
        results = self.clean_401_files(files)
        
        print("\n" + "=" * 50)
        print("清理流程完成")
        print("=" * 50)
        
        return results
    
    def run_quick_cleanup(self) -> Dict[str, Any]:
        """
        快速清理流程:
        1. 只获取非活跃文件
        2. 批量查询状态
        3. 清理401文件
        """
        print("=" * 50)
        print("开始CPA快速清理流程")
        print("=" * 50)
        
        # 1. 获取非活跃文件
        print("\n[1/3] 获取非活跃文件列表...")
        files = self.fetch_non_active_files()
        print(f"获取到 {len(files)} 个非活跃文件")
        
        codex_files = [f for f in files if self._supports_active_check(f)]
        print(f"其中Codex文件: {len(codex_files)}")
        
        if not codex_files:
            print("没有Codex文件，退出")
            return {"deleted": 0, "enabled": 0, "disabled": 0}
        
        # 2. 批量查询状态
        print("\n[2/3] 查询文件状态...")
        files = self.query_files_batch(files)
        
        # 3. 清理401文件
        print("\n[3/3] 执行清理...")
        results = self.clean_401_files(files)
        
        print("\n" + "=" * 50)
        print("快速清理流程完成")
        print("=" * 50)
        
        return results


def main():
    """主函数 - 适合GitHub Actions运行"""
    # 从环境变量获取配置
    base_url = os.environ.get("CPA_BASE_URL")
    token = os.environ.get("CPA_TOKEN")
    
    if not base_url or not token:
        print("错误: 请设置环境变量 CPA_BASE_URL 和 CPA_TOKEN")
        print("示例:")
        print("  export CPA_BASE_URL=https://your-domain.com")
        print("  export CPA_TOKEN=your-auth-token")
        sys.exit(1)
    
    # 可选配置
    concurrency = int(os.environ.get("CPA_CONCURRENCY", "4"))
    mode = os.environ.get("CPA_MODE", "full")  # full 或 quick
    
    try:
        cleaner = CPAuthCleaner(base_url, token, concurrency)
        
        if mode == "quick":
            results = cleaner.run_quick_cleanup()
        else:
            results = cleaner.run_full_cleanup()
        
        # 输出结果供GitHub Actions使用
        print(f"\n::set-output name=deleted::{results['deleted']}")
        print(f"::set-output name=enabled::{results['enabled']}")
        print(f"::set-output name=disabled::{results['disabled']}")
        
        # 如果有删除操作，返回成功状态码
        sys.exit(0)
        
    except Exception as e:
        print(f"执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
