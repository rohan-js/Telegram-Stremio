from dataclasses import dataclass


EBML_ID = 0x1A45DFA3
SEGMENT_ID = 0x18538067
SEEK_HEAD_ID = 0x114D9B74
SEEK_ID = 0x4DBB
SEEK_ID_ID = 0x53AB
SEEK_POSITION_ID = 0x53AC
CUES_ID = 0x1C53BB6B

SEEK_HEAD_BYTES = SEEK_HEAD_ID.to_bytes(4, "big")
CUES_BYTES = CUES_ID.to_bytes(4, "big")


@dataclass(frozen=True)
class MkvSeekRiskResult:
    risk: bool = False
    reason: str | None = None
    first_seek_targets: tuple[int, ...] = ()
    tail_seek_targets: tuple[int, ...] = ()
    tail_has_cues: bool = False


def _vint_length(first_byte: int, max_len: int) -> int | None:
    mask = 0x80
    for length in range(1, max_len + 1):
        if first_byte & mask:
            return length
        mask >>= 1
    return None


def _read_element_id(data: bytes, pos: int) -> tuple[int, int] | None:
    if pos >= len(data):
        return None
    length = _vint_length(data[pos], 4)
    if not length or pos + length > len(data):
        return None
    return int.from_bytes(data[pos:pos + length], "big"), pos + length


def _read_vint_size(data: bytes, pos: int) -> tuple[int | None, int] | None:
    if pos >= len(data):
        return None
    first = data[pos]
    length = _vint_length(first, 8)
    if not length or pos + length > len(data):
        return None
    marker_mask = 1 << (8 - length)
    value = first & (marker_mask - 1)
    for b in data[pos + 1:pos + length]:
        value = (value << 8) | b
    unknown_value = (1 << (7 * length)) - 1
    return (None if value == unknown_value else value), pos + length


def _iter_elements(data: bytes, start: int, end: int):
    pos = max(0, start)
    end = min(len(data), max(start, end))
    while pos < end:
        element_start = pos
        id_read = _read_element_id(data, pos)
        if not id_read:
            return
        element_id, pos = id_read

        size_read = _read_vint_size(data, pos)
        if not size_read:
            return
        size, payload_start = size_read

        if size is None:
            payload_end = end
        else:
            payload_end = payload_start + size
            if payload_end > end:
                return

        yield element_id, element_start, payload_start, payload_end

        if payload_end <= element_start:
            return
        pos = payload_end


def _find_segment_payload(head: bytes) -> tuple[int, int] | None:
    for element_id, _element_start, payload_start, payload_end in _iter_elements(head, 0, len(head)):
        if element_id == SEGMENT_ID:
            return payload_start, payload_end
        if element_id not in {EBML_ID, SEGMENT_ID}:
            break
    return None


def _parse_seek_targets(data: bytes, payload_start: int, payload_end: int) -> tuple[int, ...]:
    targets: list[int] = []
    for element_id, _seek_start, seek_payload_start, seek_payload_end in _iter_elements(data, payload_start, payload_end):
        if element_id != SEEK_ID:
            continue
        for child_id, _child_start, child_payload_start, child_payload_end in _iter_elements(data, seek_payload_start, seek_payload_end):
            if child_id != SEEK_ID_ID:
                continue
            raw_id = data[child_payload_start:child_payload_end]
            if raw_id:
                targets.append(int.from_bytes(raw_id, "big"))
            break
    return tuple(targets)


def _first_seek_head_targets(head: bytes) -> tuple[int, ...]:
    segment = _find_segment_payload(head)
    if not segment:
        return ()

    segment_start, segment_end = segment
    for element_id, _element_start, payload_start, payload_end in _iter_elements(head, segment_start, segment_end):
        if element_id == SEEK_HEAD_ID:
            return _parse_seek_targets(head, payload_start, payload_end)
        if element_id not in {0xEC, 0xBF}:  # Void / CRC-32
            continue
    return ()


def _tail_seek_head_targets(tail: bytes) -> tuple[int, ...]:
    targets: list[int] = []
    search_from = 0
    while True:
        idx = tail.find(SEEK_HEAD_BYTES, search_from)
        if idx < 0:
            break
        for element_id, _element_start, payload_start, payload_end in _iter_elements(tail, idx, len(tail)):
            if element_id == SEEK_HEAD_ID:
                targets.extend(_parse_seek_targets(tail, payload_start, payload_end))
            break
        search_from = idx + 1
    return tuple(targets)


def analyze_mkv_seek_risk(head: bytes, tail: bytes) -> MkvSeekRiskResult:
    """Detect MKVs whose first SeekHead hides Cues behind an end SeekHead.

    This intentionally returns no warning when the sample is inconclusive. The
    scanner is used in the Telegram ingestion path, so false negatives are safer
    than false positives or blocking the channel reply.
    """
    try:
        if not head or SEGMENT_ID.to_bytes(4, "big") not in head[:4096]:
            return MkvSeekRiskResult()

        first_targets = _first_seek_head_targets(head)
        if not first_targets:
            return MkvSeekRiskResult()

        if CUES_ID in first_targets:
            return MkvSeekRiskResult(first_seek_targets=first_targets)

        tail_targets = _tail_seek_head_targets(tail or b"")
        tail_has_cues = CUES_BYTES in (tail or b"")
        if CUES_ID in tail_targets or tail_has_cues:
            return MkvSeekRiskResult(
                risk=True,
                reason="first SeekHead does not point to Cues; seek metadata is only discoverable near the file end",
                first_seek_targets=first_targets,
                tail_seek_targets=tail_targets,
                tail_has_cues=tail_has_cues,
            )

        return MkvSeekRiskResult(
            first_seek_targets=first_targets,
            tail_seek_targets=tail_targets,
            tail_has_cues=tail_has_cues,
        )
    except Exception:
        return MkvSeekRiskResult()
