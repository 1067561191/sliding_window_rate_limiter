import time

from django.utils.deprecation import MiddlewareMixin
from redis import Redis
from typing import Callable, Tuple

from rest_framework.viewsets import ModelViewSet


redis_conn = Redis(
    host="127.0.0.1",
    port=6379,
    db=0,
    password="123456",
    encoding="utf-8",
    decode_responses=True,
    socket_timeout=0.5,
    socket_connect_timeout=0.5
)


class RateLimitException(Exception):
    pass

LUA_SCRIPT = """
-- 滑动窗口限流Lua脚本
-- 参数说明：
-- KEYS[1]：redis zset的完整key（key前缀+分隔符+用户key）
-- ARGV[1]：当前时间 毫秒
-- ARGV[2]：窗口时间 毫秒
-- ARGV[3]：请求频率

local window_start = ARGV[1] - ARGV[2]
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', window_start)
local count = redis.call('ZCARD', KEYS[1])
if count < tonumber(ARGV[3]) then
    redis.call('ZADD', KEYS[1], ARGV[1], ARGV[1])
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2])/1000)
    return 0
else
    return 1
end
"""

class SlidingWindowRateLimiter:

    @classmethod
    def by_user(cls, key: str, get_user_key_func: Callable, interval: int, frequency: int, separator: str = "_"):
        """
        接口用户级限流

        :param key: redis zset的key前缀
        :param get_user_key_func: 从request里获取用户唯一key的方法
        :param interval: X秒内
        :param frequency: Y次访问
        :param separator: redis的key之间的分隔符
        :return:
        """

        script_sha = redis_conn.script_load(LUA_SCRIPT)

        def decorator(func: Callable):

            def wrapper(instance, request, *args, **kwargs):
                user_key = get_user_key_func(request)
                redis_key = key + separator + user_key
                result = int(redis_conn.evalsha(
                    script_sha,
                    1,
                    redis_key,
                    int(time.time_ns()/1e3),
                    interval * 1e3,
                    frequency
                ))
                if 0 == result:
                    response = func(instance, request, *args, **kwargs)
                    return response
                raise RateLimitException("服务器繁忙，请稍后再试...")
            return wrapper

        return decorator


class SlidingWindowRateLimiterMiddleware(MiddlewareMixin):

    key_prefix = "rate_limit_sliding_window"
    need_limit_urls = [
        ("GET", "/openapi/v1/product/", 60, 60)
    ]
    script_sha = redis_conn.script_load(LUA_SCRIPT)

    def get_client_ip(self, request) -> str:
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR', '')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0].strip()
        else:
            ip = request.META.get('REMOTE_ADDR', 'unknown')
        return ip

    def is_need_limit(self, request) -> Tuple[bool, str, int, int]:
        for method, path_prefix, interval, frequency in self.need_limit_urls:
            if request.method.upper() == method and request.path.startswith(path_prefix):
                return True, f"{method}_{path_prefix}", interval, frequency
        return False, "", 0, 0

    def process_request(self, request):
        need_limit, key_subfix, interval, frequency = self.is_need_limit(request)
        if not need_limit:
            return
        client_ip = self.get_client_ip(request)
        if "unkonwn" == client_ip:
            return
        redis_key = f"{self.key_prefix}_{key_subfix}_{client_ip}"
        result = int(redis_conn.evalsha(
            self.script_sha,
            1,
            redis_key,
            int(time.time_ns() / 1e3),
            interval * 1e3,
            frequency
        ))
        if 0 == result:
            return
        raise RateLimitException("服务器繁忙，请稍后再试...")


class ProductModelViewSet(ModelViewSet):

    @SlidingWindowRateLimiter.by_user(key="product_list", get_user_key_func=lambda x: x.user.id, interval=60, frequency=60)
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)