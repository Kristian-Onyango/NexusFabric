import socket
import json
import base64
import os
import time
import argparse


def node_id():
    """
    InnerCore NodeID is [32]byte in Go.
    Go's json package ONLY base64-encodes byte SLICES ([]byte).
    A fixed-size array like [32]byte is NOT a slice, so it falls back
    to default array encoding: a plain JSON array of 32 integers (0-255).
    Sending base64 here causes json.Unmarshal to fail on the Go side,
    and the packet gets silently dropped — no ACK, no error, just a timeout.
    """
    return list(os.urandom(32))


def build_message(source_id, destination_id, text):
    request_id = f"python-{time.time_ns()}"

    return {
        "header": {
            "version": 2,
            "request_id": request_id,
            "packet_type": "MSG",
            "ttl": 8,
            "source_node_id": source_id,
            "destination_node_id": destination_id,
            "timestamp": int(time.time()),
            "payload_length": len(text.encode()),
        },

        "network": {
            "source_region": "KE-Nairobi",
            "destination_region": ""
        },

        "capabilities": {},

        "payload": {
            "text": text
        }
    }


def main():
    parser = argparse.ArgumentParser(
        description="Simple Python MVP for testing InnerCore Layer 4 messaging"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=51000,
        help="InnerCore message port (default: 51000)"
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="InnerCore host (default: 127.0.0.1)"
    )

    args = parser.parse_args()

    # Python's own UDP socket.
    # We use an ephemeral local port so it doesn't conflict
    # with InnerCore's 51000 / 51010 ports.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sock.bind(("0.0.0.0", 0))

    local_ip, local_port = sock.getsockname()

    source_id = node_id()

    # We don't need the real destination NodeID for this
    # basic Layer 4 test because the current Go receiver
    # processes the MSG and sends an ACK based on SourceNodeID.
    destination_id = [0] * 32

    print("=" * 60)
    print("InnerCore Python Text MVP")
    print("=" * 60)

    print(f"Target       : {args.host}:{args.port}")
    print(f"Python port  : {local_port}")
    print(f"Python NodeID: {bytes(source_id[:12]).hex()}...")
    print()

    text = input("Message: ")

    packet = build_message(
        source_id,
        destination_id,
        text
    )

    data = json.dumps(packet).encode()

    print()
    print(f"[SEND] → {args.host}:{args.port}")
    print(f"[TEXT] {text}")

    sock.sendto(
        data,
        (args.host, args.port)
    )

    sock.settimeout(5)

    try:
        response, address = sock.recvfrom(8192)

        print()
        print(f"[RECV] ← {address[0]}:{address[1]}")

        ack = json.loads(response.decode())

        print(f"[TYPE] {ack.get('header', {}).get('packet_type')}")
        print(f"[REQUEST] {ack.get('header', {}).get('request_id')}")

        if ack.get("header", {}).get("packet_type") == "ACK":
            print()
            print("✅ INNERCORE LAYER 4 TEST PASSED")
            print("Go node received the message and returned an ACK.")

        else:
            print()
            print("⚠️ Received a packet, but it was not an ACK.")

    except socket.timeout:
        print()
        print("❌ TIMEOUT")
        print("No ACK was received within 5 seconds.")

    finally:
        sock.close()


if __name__ == "__main__":
    main()