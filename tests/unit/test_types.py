import base64
import json
from datetime import datetime, date, timedelta, timezone

import pytest

from pytucky.core.types import (
    IntCodec,
    StrCodec,
    FloatCodec,
    BoolCodec,
    BytesCodec,
    DateCodec,
    DatetimeCodec,
    TimedeltaCodec,
    ListCodec,
    DictCodec,
    SerializationError,
    TypeCode,
    TypeRegistry,
)


# 基本 codec roundtrip 测试

def test_int_codec_roundtrip():
    codec = IntCodec()
    v = 1234567890123
    data = codec.encode(v)
    out, n = codec.decode(data)
    assert out == v and n == 8


def test_str_codec_roundtrip_and_empty():
    codec = StrCodec()
    v = "hello 世界"
    data = codec.encode(v)
    out, n = codec.decode(data)
    assert out == v
    # 空字符串
    data2 = codec.encode("")
    out2, n2 = codec.decode(data2)
    assert out2 == ""


def test_float_codec_roundtrip():
    codec = FloatCodec()
    v = 3.1415926
    data = codec.encode(v)
    out, n = codec.decode(data)
    assert pytest.approx(out, rel=1e-12) == v


def test_bool_codec_roundtrip():
    codec = BoolCodec()
    for v in (True, False):
        data = codec.encode(v)
        out, n = codec.decode(data)
        assert out is v


def test_bytes_codec_roundtrip():
    codec = BytesCodec()
    v = b"\x00\x01hello"
    data = codec.encode(v)
    out, n = codec.decode(data)
    assert out == v


# Date/Datetime/Timedelta tests

def test_date_codec_roundtrip_pre_epoch_and_epoch():
    codec = DateCodec()
    # 1960-01-01 (pre-epoch)
    v1 = date(1960, 1, 1)
    data1 = codec.encode(v1)
    out1, n1 = codec.decode(data1)
    assert out1 == v1

    # epoch
    v2 = date(1970, 1, 1)
    data2 = codec.encode(v2)
    out2, n2 = codec.decode(data2)
    assert out2 == v2


def test_timedelta_codec_various_durations():
    codec = TimedeltaCodec()
    cases = [timedelta(seconds=0), timedelta(seconds=1, microseconds=500), timedelta(days=365), timedelta(days=-1, seconds=1)]
    for v in cases:
        data = codec.encode(v)
        out, n = codec.decode(data)
        assert out == v


def test_datetime_codec_naive_and_aware_utc_and_offset():
    codec = DatetimeCodec()
    # naive datetime: roundtrip as naive (system local); decode will produce naive
    naive = datetime(2020, 2, 29, 12, 34, 56, 123456)
    data_n = codec.encode(naive)
    out_n, n_n = codec.decode(data_n)
    # 比较时间戳相等（因为 naive 保存为本地时间戳，直接比较可能受时区影响）；比较 replace(tzinfo=None)
    assert out_n.replace(tzinfo=None) == naive.replace(tzinfo=None)

    # aware UTC
    aware_utc = datetime(2020, 2, 29, 12, 34, 56, 123456, tzinfo=timezone.utc)
    data_u = codec.encode(aware_utc)
    out_u, n_u = codec.decode(data_u)
    assert out_u.tzinfo == timezone.utc
    assert out_u == aware_utc

    # aware +08:00
    tz8 = timezone(timedelta(hours=8))
    aware_8 = datetime(2020, 2, 29, 20, 34, 56, 123456, tzinfo=tz8)
    data_8 = codec.encode(aware_8)
    out_8, n_8 = codec.decode(data_8)
    assert out_8.tzinfo == tz8
    assert out_8 == aware_8


# List/Dict codecs

def test_list_dict_codec_roundtrip_and_non_json_serializable():
    lcodec = ListCodec()
    dcodec = DictCodec()

    data_list = [1, "a", True, 3.14, None, [1, 2]]
    data = lcodec.encode(data_list)
    out, n = lcodec.decode(data)
    assert out == data_list

    data_dict = {"x": 1, "y": [1, 2], "s": "中文"}
    dd = dcodec.encode(data_dict)
    outd, nd = dcodec.decode(dd)
    assert outd == data_dict

    # 非 JSON 可序列化（datetime）应当在 json.dumps 时抛出 TypeError -> 被封装为 SerializationError? 实际上 ListCodec.encode 会调用 json.dumps 并让 TypeError 冒出
    with pytest.raises(TypeError):
        lcodec.encode([datetime.now()])
    with pytest.raises(TypeError):
        dcodec.encode({"t": datetime.now()})


# 错误处理

def test_codec_type_mismatch_and_truncated_data_raises():
    with pytest.raises(SerializationError):
        IntCodec().encode("not-an-int")
    # truncated int
    with pytest.raises(SerializationError):
        IntCodec().decode(b"\x01\x02")

    with pytest.raises(SerializationError):
        StrCodec().encode(123)
    with pytest.raises(SerializationError):
        StrCodec().decode(b"\x00")

    with pytest.raises(SerializationError):
        FloatCodec().encode("no")
    with pytest.raises(SerializationError):
        FloatCodec().decode(b"\x00")

    with pytest.raises(SerializationError):
        BoolCodec().encode(1)
    with pytest.raises(SerializationError):
        BoolCodec().decode(b"")

    with pytest.raises(SerializationError):
        BytesCodec().encode("not-bytes")
    with pytest.raises(SerializationError):
        BytesCodec().decode(b"\x00\x00\x00")


# TypeRegistry tests

def test_type_registry_get_codec_and_unknown():
    code, codec = TypeRegistry.get_codec(int)
    assert code == TypeCode.INT
    assert isinstance(codec, IntCodec)

    with pytest.raises(SerializationError):
        TypeRegistry.get_codec(set)  # unsupported type


def test_type_code_roundtrip_and_get_codec_by_code():
    # pick a known type code
    t = TypeRegistry.get_type_from_code(TypeCode.STR)
    assert t is str
    code, codec = TypeRegistry.get_codec_by_code(TypeCode.STR)
    assert code == TypeCode.STR
    assert isinstance(codec, StrCodec)

    with pytest.raises(SerializationError):
        TypeRegistry.get_type_from_code(999)  # invalid code


def test_serialize_deserialize_for_text_roundtrips_and_none_and_empty_string():
    # datetime
    now = datetime.now(timezone.utc)
    s = TypeRegistry.serialize_for_text(now, datetime)
    got = TypeRegistry.deserialize_from_text(s, datetime)
    assert got == now

    # date
    d = date(1999, 12, 31)
    sd = TypeRegistry.serialize_for_text(d, date)
    gotd = TypeRegistry.deserialize_from_text(sd, date)
    assert gotd == d

    # bytes
    b = b"abc\x00"
    sb = TypeRegistry.serialize_for_text(b, bytes)
    # text serializer returns base64 string
    assert isinstance(sb, str)
    gotb = TypeRegistry.deserialize_from_text(sb, bytes)
    assert gotb == b

    # timedelta
    td = timedelta(days=2, seconds=3, microseconds=400)
    std = TypeRegistry.serialize_for_text(td, timedelta)
    gott = TypeRegistry.deserialize_from_text(std, timedelta)
    assert gott == td

    # list/dict
    lst = [1, 2, {"x": 3}]
    sl = TypeRegistry.serialize_for_text(lst, list)
    assert isinstance(sl, str)
    gotl = TypeRegistry.deserialize_from_text(sl, list)
    assert gotl == lst

    dd = {"a": 1}
    sd2 = TypeRegistry.serialize_for_text(dd, dict)
    gotd2 = TypeRegistry.deserialize_from_text(sd2, dict)
    assert gotd2 == dd

    # None remains None
    assert TypeRegistry.serialize_for_text(None, int) is None
    assert TypeRegistry.deserialize_from_text(None, int) is None

    # 空字符串针对 str 类型返回空字符串（Bug 修复验证）
    assert TypeRegistry.deserialize_from_text("", str) == ""


def test_get_type_name_and_get_type_by_name_known_and_unknown():
    assert TypeRegistry.get_type_name(int) == 'int'
    assert TypeRegistry.get_type_name(set) == 'str'  # unknown -> default 'str'

    assert TypeRegistry.get_type_by_name('int') is int
    assert TypeRegistry.get_type_by_name('unknown') is str
