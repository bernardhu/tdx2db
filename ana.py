import struct

file_path = "./datatool/vipdoc/tdxgp/gpsz000009.dat"  # 换成你的实际路径

with open(file_path, "rb") as f:
    i = 0
    while True:
        raw = f.read(13)
        if len(raw) < 13:
            break

        # 1B type + 4B date
        t, date = struct.unpack("<B I", raw[:5])
        # 4B float32 + 4B float32
        val1 = struct.unpack("<f", raw[5:9])[0]
        val2 = struct.unpack("<f", raw[9:13])[0]

        print(
            f"{i:03d}  type=0x{t:02X}  date={date}  "
            f"val1={val1:.3f}  val2={val2:.3f}"
        )

        i += 1