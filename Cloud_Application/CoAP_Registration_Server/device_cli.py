#!/usr/bin/env python3

import os
import sys
import time
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient


INFLUX_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUX_ORG = os.getenv("INFLUXDB_ORG", "myorg")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "iot_health")

POLL_INTERVAL_SECONDS = 2
REGISTRATION_TIMEOUT_SECONDS = 90


class DeviceCLI:
    def __init__(self) -> None:
        if not INFLUX_TOKEN:
            raise RuntimeError(
                "Variabile d'ambiente INFLUXDB_TOKEN non configurata."
            )

        self.client = InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
        )

        self.query_api = self.client.query_api()

    def close(self) -> None:
        self.client.close()

    def wait_for_registration(self, expected_type: str) -> None:
        start_time = datetime.now(timezone.utc)

        readable_type = (
            "caregiver"
            if expected_type == "caregiver"
            else "patient"
        )

        print()
        print(f"Registrazione dispositivo {readable_type}")
        print("----------------------------------------")
        print("1. Accendi il dispositivo.")
        print("2. Attendi che si connetta alla rete.")
        print("3. Il dispositivo si registrerà automaticamente.")
        print()
        input("Premi INVIO dopo aver acceso il dispositivo...")

        print()
        print("In attesa della registrazione", end="", flush=True)

        deadline = time.time() + REGISTRATION_TIMEOUT_SECONDS

        try:
            while time.time() < deadline:
                device = self.find_new_registration(
                    expected_type=expected_type,
                    start_time=start_time,
                )

                if device is not None:
                    print("\n")
                    print("Dispositivo registrato correttamente.")
                    print("----------------------------------------")
                    print(f"ID:        {device['node_id']}")
                    print(f"Tipo:      {device['type']}")
                    print(f"Protocollo:{device['protocol']}")
                    print(f"Data:      {device['time']}")
                    return

                print(".", end="", flush=True)
                time.sleep(POLL_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n\nOperazione annullata.")
            return

        print("\n")
        print("Nessuna registrazione ricevuta entro il tempo previsto.")
        print("Verifica che:")
        print("- il nodo abbia connettività IPv6;")
        print("- il broker MQTT sia raggiungibile;")
        print("- il server CoAP sia in esecuzione;")
        print("- il nodo invii il tipo corretto.")

    def find_new_registration(
        self,
        expected_type: str,
        start_time: datetime,
    ) -> dict | None:

        start_iso = start_time.isoformat()

        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: time(v: "{start_iso}"))
          |> filter(fn: (r) => r["_measurement"] == "registration")
          |> filter(fn: (r) => r["_field"] == "protocol")
          |> filter(fn: (r) => r["type"] == "{expected_type}")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 1)
        '''

        tables = self.query_api.query(
            query=query,
            org=INFLUX_ORG,
        )

        for table in tables:
            for record in table.records:
                return {
                    "node_id": record.values.get("node_id", "unknown"),
                    "type": record.values.get("type", "unknown"),
                    "protocol": record.get_value(),
                    "time": record.get_time().astimezone().strftime(
                        "%d/%m/%Y %H:%M:%S"
                    ),
                }

        return None

    def list_registered_devices(self) -> None:
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -30d)
          |> filter(fn: (r) => r["_measurement"] == "registration")
          |> filter(fn: (r) => r["_field"] == "protocol")
          |> sort(columns: ["_time"], desc: true)
        '''

        tables = self.query_api.query(
            query=query,
            org=INFLUX_ORG,
        )

        devices = {}

        for table in tables:
            for record in table.records:
                node_id = record.values.get("node_id")

                if not node_id:
                    continue

                # Mantiene soltanto la registrazione più recente
                # per ciascun dispositivo.
                if node_id not in devices:
                    devices[node_id] = {
                        "node_id": node_id,
                        "type": record.values.get("type", "unknown"),
                        "protocol": record.get_value(),
                        "time": record.get_time().astimezone().strftime(
                            "%d/%m/%Y %H:%M:%S"
                        ),
                    }

        print()
        print("Dispositivi registrati")
        print("---------------------------------------------------------------")

        if not devices:
            print("Nessun dispositivo registrato negli ultimi 30 giorni.")
            return

        print(
            f"{'ID':<20}"
            f"{'TIPO':<14}"
            f"{'PROTOCOLLO':<12}"
            f"{'ULTIMA REGISTRAZIONE'}"
        )

        print("-" * 65)

        for device in devices.values():
            print(
                f"{device['node_id']:<20}"
                f"{device['type']:<14}"
                f"{device['protocol']:<12}"
                f"{device['time']}"
            )

    def run(self) -> None:
        while True:
            print()
            print("========================================")
            print(" Smart Health – Gestione dispositivi")
            print("========================================")
            print()
            print("1. Registra il mio dispositivo caregiver")
            print("2. Registra un nuovo dispositivo paziente")
            print("3. Visualizza i dispositivi registrati")
            print("0. Esci")

            choice = input("\nScelta: ").strip()

            if choice == "1":
                self.wait_for_registration("caregiver")

            elif choice == "2":
                self.wait_for_registration("patient")

            elif choice == "3":
                self.list_registered_devices()

            elif choice == "0":
                print("\nChiusura della CLI.")
                return

            else:
                print("\nScelta non valida. Inserisci 0, 1, 2 oppure 3.")


def main() -> None:
    cli = None

    try:
        cli = DeviceCLI()
        cli.run()

    except RuntimeError as exc:
        print(f"Errore di configurazione: {exc}", file=sys.stderr)
        sys.exit(1)

    except Exception as exc:
        print(f"Errore: {exc}", file=sys.stderr)
        sys.exit(1)

    finally:
        if cli is not None:
            cli.close()


if __name__ == "__main__":
    main()