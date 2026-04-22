"""
Pytucky 加密模块 - 纯 Python 实现，零外部依赖

提供三级加密支持：
- low: XOR 混淆（防随手看）
- medium: LCG 流密码（防普通用户）
- high: ChaCha20（密码学安全）
"""

from __future__ import annotations

import hashlib
import struct

from .exceptions import ConfigurationError

# 加密等级常量
ENCRYPTION_LEVELS = ('low', 'medium', 'high')

# KDF 迭代次数
KDF_ITERATIONS = {
    'low': 1,
    'medium': 1000,
    'high': 10000,
}

class CryptoProvider:
    """加密工具类"""

    @staticmethod
    def derive_key(password: str, salt: bytes, level: str) -> bytes:
        """
        从密码派生密钥

        Args:
            password: 用户密码
            salt: 随机盐（16字节）
            level: 加密等级 ('low', 'medium', 'high')

        Returns:
            32字节密钥
        """
        iterations = KDF_ITERATIONS.get(level, 1)
        key = password.encode('utf-8') + salt
        for _ in range(iterations):
            key = hashlib.sha256(key).digest()
        return key

    @staticmethod
    def compute_key_check(key: bytes) -> bytes:
        """
        计算密钥校验值（4字节）

        用于快速验证密码是否正确

        Args:
            key: 32字节密钥

        Returns:
            4字节校验值
        """
        return hashlib.sha256(key).digest()[:4]

    @staticmethod
    def verify_key(key: bytes, key_check: bytes) -> bool:
        """
        验证密钥是否正确

        Args:
            key: 32字节密钥
            key_check: 4字节校验值

        Returns:
            密钥是否匹配
        """
        return CryptoProvider.compute_key_check(key) == key_check

class XORCipher:
    """
    低级加密：XOR 混淆

    安全性：防君子不防小人，仅防随手查看
    性能税：极低（~5%）
    """

    def __init__(self, key: bytes) -> None:
        """
        初始化 XOR 加密器

        Args:
            key: 密钥（任意长度，会被扩展到 256 字节）
        """
        self.keystream = self._expand_key(key, 256)

    @staticmethod
    def _expand_key(key: bytes, length: int) -> bytes:
        """
        扩展密钥到指定长度

        使用 SHA256 链式扩展

        Args:
            key: 原始密钥
            length: 目标长度

        Returns:
            扩展后的密钥流
        """
        result = bytearray()
        h = hashlib.sha256(key).digest()
        while len(result) < length:
            result.extend(h)
            h = hashlib.sha256(h).digest()
        return bytes(result[:length])

    def encrypt(self, data: bytes) -> bytes:
        """
        加密数据

        Args:
            data: 明文数据

        Returns:
            密文数据
        """
        result = bytearray(len(data))
        keylen = len(self.keystream)
        for i, b in enumerate(data):
            result[i] = b ^ self.keystream[i % keylen]
        return bytes(result)

    def decrypt(self, data: bytes) -> bytes:
        """
        解密数据（XOR 对称，与加密相同）

        Args:
            data: 密文数据

        Returns:
            明文数据
        """
        return self.encrypt(data)

    def decrypt_at(self, offset: int, data: bytes) -> bytes:
        """
        从指定偏移位置解密数据（随机访问解密）

        XOR keystream 以 256 字节为周期循环，因此偏移量取模即可

        Args:
            offset: 数据在加密流中的起始偏移（相对于 encrypt() 起点）
            data: 需要解密的密文片段

        Returns:
            解密后的明文
        """
        result = bytearray(len(data))
        keylen = len(self.keystream)
        for i, b in enumerate(data):
            result[i] = b ^ self.keystream[(offset + i) % keylen]
        return bytes(result)

class LCGCipher:
    """
    中级加密：LCG 流密码

    使用线性同余生成器 (Linear Congruential Generator) 生成伪随机流

    安全性：防普通用户，无法抵抗专业分析
    性能税：低（~10-15%）
    """

    # LCG 参数（来自 Numerical Recipes）
    A = 1664525
    C = 1013904223
    M = 2**32

    def __init__(self, key: bytes) -> None:
        """
        初始化 LCG 加密器

        Args:
            key: 密钥（任意长度，会被哈希为 seed）
        """
        # 从 key 派生 seed
        self.seed = struct.unpack('<I', hashlib.sha256(key).digest()[:4])[0]

    def _generate_stream(self, length: int, seed: int) -> bytes:
        """
        生成伪随机流

        Args:
            length: 流长度
            seed: 随机种子

        Returns:
            伪随机字节流
        """
        result = bytearray(length)
        state = seed
        for i in range(length):
            state = (self.A * state + self.C) % self.M
            result[i] = (state >> 16) & 0xFF
        return bytes(result)

    def encrypt(self, data: bytes) -> bytes:
        """
        加密数据

        Args:
            data: 明文数据

        Returns:
            密文数据
        """
        stream = self._generate_stream(len(data), self.seed)
        return bytes(a ^ b for a, b in zip(data, stream))

    def decrypt(self, data: bytes) -> bytes:
        """
        解密数据（流密码对称，与加密相同）

        Args:
            data: 密文数据

        Returns:
            明文数据
        """
        return self.encrypt(data)

    def _advance_state(self, state: int, n: int) -> int:
        """
        将 LCG 状态快进 n 步

        使用标准的 O(log N) 快进算法，避免逐步迭代

        Args:
            state: 当前 LCG 状态
            n: 需要前进的步数

        Returns:
            快进后的 LCG 状态
        """
        acc_mult = 1
        acc_add = 0
        cur_mult = self.A
        cur_add = self.C
        m = self.M
        while n > 0:
            if n & 1:
                acc_mult = (acc_mult * cur_mult) % m
                acc_add = (acc_add * cur_mult + cur_add) % m
            cur_add = (cur_add * (cur_mult + 1)) % m
            cur_mult = (cur_mult * cur_mult) % m
            n >>= 1
        return (acc_mult * state + acc_add) % m

    def decrypt_at(self, offset: int, data: bytes) -> bytes:
        """
        从指定偏移位置解密数据（随机访问解密）

        通过 O(log N) 快进算法将 LCG 状态跳到目标偏移位置

        Args:
            offset: 数据在加密流中的起始偏移（相对于 encrypt() 起点）
            data: 需要解密的密文片段

        Returns:
            解密后的明文
        """
        # 快进 LCG 状态到 offset 位置
        state = self._advance_state(self.seed, offset)
        result = bytearray(len(data))
        for i in range(len(data)):
            state = (self.A * state + self.C) % self.M
            result[i] = data[i] ^ ((state >> 16) & 0xFF)
        return bytes(result)

class ChaCha20Cipher:
    """
    高级加密：ChaCha20

    纯 Python 实现的 ChaCha20 流密码（RFC 7539）

    安全性：密码学安全，可抵抗专业攻击
    性能税：中等（~30-50%）
    """

    def __init__(self, key: bytes, nonce: bytes | None = None) -> None:
        """
        初始化 ChaCha20 加密器

        Args:
            key: 密钥（32字节，如果不足会用 SHA256 扩展）
            nonce: 随机数（12字节，默认全零）
        """
        if len(key) != 32:
            key = hashlib.sha256(key).digest()
        self.key = key
        self.nonce = nonce if nonce else b'\x00' * 12

    @staticmethod
    def _quarter_round(state: list, a: int, b: int, c: int, d: int) -> None:
        """
        ChaCha20 四分之一轮函数

        Args:
            state: 16个32位整数的状态数组
            a, b, c, d: 状态索引
        """
        state[a] = (state[a] + state[b]) & 0xFFFFFFFF
        state[d] ^= state[a]
        state[d] = ((state[d] << 16) | (state[d] >> 16)) & 0xFFFFFFFF

        state[c] = (state[c] + state[d]) & 0xFFFFFFFF
        state[b] ^= state[c]
        state[b] = ((state[b] << 12) | (state[b] >> 20)) & 0xFFFFFFFF

        state[a] = (state[a] + state[b]) & 0xFFFFFFFF
        state[d] ^= state[a]
        state[d] = ((state[d] << 8) | (state[d] >> 24)) & 0xFFFFFFFF

        state[c] = (state[c] + state[d]) & 0xFFFFFFFF
        state[b] ^= state[c]
        state[b] = ((state[b] << 7) | (state[b] >> 25)) & 0xFFFFFFFF

    def _chacha20_block(self, counter: int) -> bytes:
        """
        生成一个 64 字节的密钥流块

        Args:
            counter: 块计数器

        Returns:
            64字节密钥流
        """
        # 常量 "expand 32-byte k"
        constants = [0x61707865, 0x3320646e, 0x79622d32, 0x6b206574]

        # 解析 key 为 8 个 32-bit words
        key_words = list(struct.unpack('<8I', self.key))

        # 解析 nonce 为 3 个 32-bit words
        nonce_words = list(struct.unpack('<3I', self.nonce))

        # 初始状态: constants (4) + key (8) + counter (1) + nonce (3)
        state = constants + key_words + [counter] + nonce_words
        working = state.copy()

        # 20 轮（10 次双轮）
        for _ in range(10):
            # 列轮
            self._quarter_round(working, 0, 4, 8, 12)
            self._quarter_round(working, 1, 5, 9, 13)
            self._quarter_round(working, 2, 6, 10, 14)
            self._quarter_round(working, 3, 7, 11, 15)
            # 对角线轮
            self._quarter_round(working, 0, 5, 10, 15)
            self._quarter_round(working, 1, 6, 11, 12)
            self._quarter_round(working, 2, 7, 8, 13)
            self._quarter_round(working, 3, 4, 9, 14)

        # 加上初始状态
        output = [(working[i] + state[i]) & 0xFFFFFFFF for i in range(16)]
        return struct.pack('<16I', *output)

    def encrypt(self, data: bytes) -> bytes:
        """
        加密数据

        Args:
            data: 明文数据

        Returns:
            密文数据
        """
        result = bytearray()
        counter = 0

        for i in range(0, len(data), 64):
            block = self._chacha20_block(counter)
            chunk = data[i:i+64]
            result.extend(a ^ b for a, b in zip(chunk, block))
            counter += 1

        return bytes(result)

    def decrypt(self, data: bytes) -> bytes:
        """
        解密数据（流密码对称，与加密相同）

        Args:
            data: 密文数据

        Returns:
            明文数据
        """
        return self.encrypt(data)

    def decrypt_at(self, offset: int, data: bytes) -> bytes:
        """
        从指定偏移位置解密数据（随机访问解密）

        ChaCha20 天然支持随机访问：每个 64 字节块由 counter 参数确定

        Args:
            offset: 数据在加密流中的起始偏移（相对于 encrypt() 起点）
            data: 需要解密的密文片段

        Returns:
            解密后的明文
        """
        block_index = offset // 64
        byte_offset = offset % 64
        result = bytearray()
        data_pos = 0

        while data_pos < len(data):
            block = self._chacha20_block(block_index)
            # 从 block 的 byte_offset 位置开始使用
            available = block[byte_offset:]
            chunk = data[data_pos:data_pos + len(available)]
            result.extend(a ^ b for a, b in zip(chunk, available))
            data_pos += len(chunk)
            block_index += 1
            byte_offset = 0  # 后续块从头开始

        return bytes(result)

# 类型别名
CipherType = XORCipher | LCGCipher | ChaCha20Cipher

def get_cipher(level: str, key: bytes) -> CipherType:
    """
    获取对应等级的加密器

    Args:
        level: 加密等级 ('low', 'medium', 'high')
        key: 密钥

    Returns:
        加密器实例

    Raises:
        ConfigurationError: 无效的加密等级
    """
    if level == 'low':
        return XORCipher(key)
    if level == 'medium':
        return LCGCipher(key)
    if level == 'high':
        return ChaCha20Cipher(key)
    raise ConfigurationError(
        f"Invalid encryption level: {level}. Must be one of {ENCRYPTION_LEVELS}",
        details={'level': level, 'valid_levels': ENCRYPTION_LEVELS}
    )

def get_encryption_level_code(level: str) -> int:
    """
    获取加密等级的数值代码（用于 flags）

    Args:
        level: 加密等级 ('low', 'medium', 'high')

    Returns:
        数值代码 (1, 2, 3)
    """
    codes = {'low': 1, 'medium': 2, 'high': 3}
    return codes.get(level, 0)

def get_encryption_level_name(code: int) -> str | None:
    """
    从数值代码获取加密等级名称

    Args:
        code: 数值代码 (1, 2, 3)

    Returns:
        加密等级名称，无效代码返回 None
    """
    names = {1: 'low', 2: 'medium', 3: 'high'}
    return names.get(code)
