#!/usr/bin/env python3

import os
import sys
import time
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point


INFLUX_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN", "-dDRkdv-5FuOWXeJSpnVr_gjHKrhGw2CyYp-MezlwAyIU5cwKbaQKyPV5--0hRj0kZHJ8W_cXymV6JaB_vBo6g==")
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

                    if expected_type == "patient":
                        print()
                        risposta = input("Vuoi associare un nome a questo paziente? (s/n): ").strip().lower()
                        if risposta == "s":
                            self._set_patient_name(device["node_id"])

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

    def find_new_registration(self, expected_type: str, start_time: datetime,) -> dict | None:

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

        tables = self.query_api.query(query=query, org=INFLUX_ORG,)

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

                # Mantiene soltanto la registrazione più recente
                # per ciascun dispositivo.
                if node_id and node_id not in devices:
                    devices[node_id] = {
                        "node_id": node_id,
                        "type": record.values.get("type", "unknown"),
                        "protocol": record.get_value(),
                        "time": record.get_time().astimezone().strftime(
                            "%d/%m/%Y %H:%M:%S"
                        ),
                    }
        names = self._get_all_patient_names()

        print()
        print("Dispositivi registrati")
        print("---------------------------------------------------------------")
        print(f"{'ID':<20}{'NOME':<20}{'TIPO':<14}{'PROTOCOLLO':<12}{'ULTIMA REG.'}")
        print("---------------------------------------------------------------")

        if not devices:
            print("Nessun dispositivo registrato negli ultimi 30 giorni.")
            return

        for d in devices.values():
            nome = names.get(d["node_id"], "-")
            print(f"{d['node_id']:<20}{nome:<20}{d['type']:<14}{d['protocol']:<12}{d['time']}")


    #Gestione nomi 
    def set_patient_name_interactive(self) -> None:
        """Flusso CLI per associare un nome a un paziente già registrato."""
        self.list_registered_devices()
        print()
        node_id = input("Inserisci l'ID del paziente a cui associare un nome: ").strip()
        if not node_id:
            print("ID non valido.")
            return
        self._set_patient_name(node_id)
 
    def _set_patient_name(self, node_id: str) -> None:
        nome = input(f"Nome da associare a {node_id}: ").strip()
        if not nome:
            print("Nome non valido.")
            return
        point = (
            Point("patient_info")
            .tag("node_id", node_id)
            .field("name", nome)
        )
        self.write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        print(f"Nome '{nome}' associato al paziente {node_id}.")
 
    def _get_all_patient_names(self) -> dict:
        """Restituisce {node_id: nome} per tutti i pazienti con nome associato."""
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -365d)
          |> filter(fn: (r) => r["_measurement"] == "patient_info")
          |> filter(fn: (r) => r["_field"] == "name")
          |> group(columns: ["node_id"])
          |> last()
        '''
        tables = self.query_api.query(query=query, org=INFLUX_ORG)
        result = {}
        for table in tables:
            for record in table.records:
                node_id = record.values.get("node_id")
                if node_id:
                    result[node_id] = record.get_value()
        return result
 
    def _get_patient_name(self, node_id: str) -> str:
        names = self._get_all_patient_names()
        return names.get(node_id, node_id)

    #gestione stato nodi
    def show_node_status(self) -> None:
        """Mostra l'ultimo heartbeat e lo stato di ogni nodo."""
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "heartbeat")
          |> filter(fn: (r) => r["_field"] == "state")
          |> group(columns: ["node_id"])
          |> last()
        '''

        tables_type = self.query_api.query(query=query_type, org=INFLUX_ORG)
        directory = {}
        for table in tables_type:
            for record in table.records:
                node_id = record.values.get("node_id")
                if node_id:
                    directory[node_id] = {
                        "type": record.values.get("type", "unknown"),
                        "last_reg": record.get_time(),
                    }

        #last event - online/offline 
        query_act = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -7d)
          |> filter(fn: (r) => r["_measurement"] == "node_activity")
          |> filter(fn: (r) => r["_field"] == "event")
          |> group(columns: ["node_id"])
          |> last()
        '''
        tables_act = self.query_api.query(query=query_act, org=INFLUX_ORG)
        activity = {}
        for table in tables_act:
            for record in table.records:
                node_id = record.values.get("node_id")
                if node_id:
                    activity[node_id] = {
                        "event": record.get_value(),
                        "time":  record.get_time(),
                    }

        #failure status
        query_fail = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -24h)
          |> filter(fn: (r) => r["_measurement"] == "failure_status")
          |> filter(fn: (r) => r["_field"] == "severity")
          |> group(columns: ["node_id"])
          |> last()
        '''
        tables_fail = self.query_api.query(query=query_fail, org=INFLUX_ORG)
        failure_status = {}
        for table in tables_fail:
            for record in table.records:
                failure_status[record.values.get("node_id")] = record.get_value()
 
        names = self._get_all_patient_names()
        now = datetime.now(timezone.utc)
 
        print()
        print("Directory nodi (registrati via CoAP)")
        print("-" * 85)
        print(f"{'ID':<20}{'NOME':<18}{'TIPO':<12}{'CONNESSIONE':<12}{'ULTIMO EVENTO':<22}{'FAILURE'}")
        print("-" * 85)
 
        if not directory:
            print("Nessun nodo registrato nella directory")
            return
 
        for node_id, info in directory.items():
            nome = names.get(node_id, "-")
            tipo = info["type"]
            act = activity.get(node_id)
            failure = failure_status.get(node_id, "-")
 
            if act is None:
                # Mai visto su MQTT in questa sessione
                connessione = "MAI ONLINE"
                ultimo      = "-"
            elif act["event"] == "ONLINE":
                connessione = "ONLINE"
                elapsed = (now - act["time"]).total_seconds()
                ultimo = f"{elapsed:.0f}s fa"
            else:
                connessione = "OFFLINE"
                ultimo = act["time"].astimezone().strftime("%d/%m %H:%M:%S")
 
            # Se failure detection lo segnala, prevale
            if fail in ("CRITICAL", "WARNING"):
                connessione = f"OFFLINE ({fail})"
 
            print(f"{node_id:<20}{nome:<18}{tipo:<12}{connessione:<12}{ultimo:<22}{fail}")

    #Gestione allarmi attivi
    def show_active_alarms(self) -> None:
        """Mostra i pazienti con allarme FALL attivo (non ancora RESOLVED)."""
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1h)
          |> filter(fn: (r) => r["_measurement"] == "alarm")
          |> filter(fn: (r) => r["_field"] == "event")
          |> group(columns: ["node_id"])
          |> last()
        '''
        tables = self.query_api.query(query=query, org=INFLUX_ORG)
 
        names = self._get_all_patient_names()
        active = []
 
        for table in tables:
            for record in table.records:
                if record.get_value() == "FALL":
                    node_id = record.values.get("node_id")
                    active.append({
                        "node_id": node_id,
                        "nome":    names.get(node_id, "-"),
                        "time":    record.get_time().astimezone().strftime("%d/%m/%Y %H:%M:%S"),
                    })
 
        print()
        print("Allarmi attivi")
        print("-" * 55)
 
        if not active:
            print("Nessun allarme attivo.")
            return
 
        print(f"{'ID PAZIENTE':<20}{'NOME':<18}{'RILEVATO ALLE'}")
        print("-" * 55)
        for a in active:
            print(f"{a['node_id']:<20}{a['nome']:<18}{a['time']}")

    #Storico allarmi
    def show_alarm_history(self) -> None:
        """Mostra gli ultimi 20 eventi alarm (FALL e RESOLVED)."""
        query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -7d)
          |> filter(fn: (r) => r["_measurement"] == "alarm")
          |> filter(fn: (r) => r["_field"] == "event")
          |> filter(fn: (r) => r["_value"] == "FALL" or r["_value"] == "RESOLVED")
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 20)
        '''
        tables = self.query_api.query(query=query, org=INFLUX_ORG)
 
        names = self._get_all_patient_names()
        events = []
 
        for table in tables:
            for record in table.records:
                node_id = record.values.get("node_id")
                events.append({
                    "node_id": node_id,
                    "nome":    names.get(node_id, "-"),
                    "event":   record.get_value(),
                    "time":    record.get_time().astimezone().strftime("%d/%m/%Y %H:%M:%S"),
                })
 
        print()
        print("Storico allarmi (ultimi 7 giorni, max 20 eventi)")
        print("-" * 65)
 
        if not events:
            print("Nessun evento negli ultimi 7 giorni.")
            return
 
        print(f"{'DATA/ORA':<22}{'ID PAZIENTE':<20}{'NOME':<15}{'EVENTO'}")
        print("-" * 65)
        for e in events:
            print(f"{e['time']:<22}{e['node_id']:<20}{e['nome']:<15}{e['event']}")

    def run(self) -> None:
        while True:
            print()
            print("========================================")
            print("  SmartGuard – Gestione dispositivi")
            print("========================================")
            print()
            print("--- Registrazione ---")
            print("1. Registra il mio dispositivo caregiver")
            print("2. Registra un nuovo dispositivo paziente")
            print("3. Visualizza dispositivi registrati")
            print()
            print("--- Pazienti ---")
            print("4. Associa nome a un paziente")
            print()
            print("--- Monitoraggio ---")
            print("5. Visualizza stato nodi")
            print("6. Visualizza allarmi attivi")
            print("7. Storico allarmi")
            print()
            print("0. Esci")

            choice = input("\nScelta: ").strip()

            if choice == "1": self.wait_for_registration("caregiver")
            elif choice == "2": self.wait_for_registration("patient")
            elif choice == "3": self.list_registered_devices()
            elif choice == "4": self.set_patient_name_interactive()
            elif choice == "5": self.show_node_status()
            elif choice == "6": self.show_active_alarms()
            elif choice == "7": self.show_alarm_history()
            elif choice == "0":
                print("\nChiusura della CLI.")
                return
            else:
                print("\nScelta non valida.")

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