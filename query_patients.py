"""Simple test query: list top 10 patients."""

import sys

from db import get_config, connect


def main():
    config = get_config()
    print(f"Connecting to {config['server']} as {config['user']}...")
    conn = connect(config)
    print("Connected.\n")

    cursor = conn.cursor()
    cursor.execute("SELECT TOP 10 PatientSer, PatientId, LastName, FirstName FROM Patient ORDER BY PatientSer")

    print(f"{'PatientSer':<12} {'PatientId':<15} {'LastName':<20} {'FirstName':<20}")
    print("-" * 70)
    for row in cursor.fetchall():
        ser, pid, last, first = row
        print(f"{ser!s:<12} {(pid or ''):<15} {(last or ''):<20} {(first or ''):<20}")

    conn.close()


if __name__ == "__main__":
    main()
