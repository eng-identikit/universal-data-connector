# Documentazione Connettori Industriali

**[🇬🇧 English](IndustrialConnectors.md)** | **[🇮🇹 Italiano](IndustrialConnectors.it.md)**

---

Questo documento fornisce informazioni dettagliate su tutti i connettori di protocolli industriali disponibili nell'Universal Data Connector.

## Table of Contents

1. [Modbus Connector](#modbus-connector)
2. [Siemens S7 Connector](#siemens-s7-connector)
3. [EtherCAT Connector](#ethercat-connector)
4. [PROFINET Connector](#profinet-connector)
5. [BACnet Connector](#bacnet-connector)
6. [FINS (Omron) Connector](#fins-omron-connector)
7. [MELSEC (Mitsubishi) Connector](#melsec-mitsubishi-connector)
8. [CIP/EtherNet/IP Connector](#cipethernet-ip-connector)
9. [Serial Connector](#serial-connector)

---

## Modbus Connector

**Type:** `modbus`  
**Library:** `modbus-serial`  
**Protocols:** Modbus TCP, Modbus RTU

### Description
Modbus is one of the most widely used protocols in industrial automation, supporting both TCP/IP and serial communication.

### Configuration Example

#### Modbus TCP
```json
{
  "id": "modbus-plc-1",
  "type": "modbus",
  "enabled": true,
  "config": {
    "connectionType": "tcp",
    "host": "192.168.1.10",
    "port": 502,
    "unitId": 1,
    "pollingInterval": 1000,
    "timeout": 5000,
    "registers": [
      {
        "name": "temperature",
        "address": 0,
        "type": "holding",
        "dataType": "float",
        "count": 2
      },
      {
        "name": "pressure",
        "address": 10,
        "type": "input",
        "dataType": "uint16",
        "count": 1
      },
      {
        "name": "valve_open",
        "address": 0,
        "type": "coil",
        "dataType": "bool",
        "count": 1
      }
    ]
  }
}
```

#### Modbus RTU
```json
{
  "id": "modbus-rtu-1",
  "type": "modbus",
  "enabled": true,
  "config": {
    "connectionType": "rtu",
    "serialPort": "/dev/ttyUSB0",
    "baudRate": 9600,
    "dataBits": 8,
    "stopBits": 1,
    "parity": "none",
    "unitId": 1,
    "pollingInterval": 1000,
    "registers": [...]
  }
}
```

### Register Types
- `holding`: Read/Write registers
- `input`: Read-only input registers
- `coil`: Read/Write coils (single bits)
- `discrete`: Read-only discrete inputs

### Data Types
- `uint16`, `int16`: 16-bit integers
- `uint32`, `int32`: 32-bit integers
- `float`: 32-bit floating point
- `bool`: Boolean

---

## Siemens S7 Connector

**Type:** `s7` or `siemens-s7`  
**Library:** `nodes7`  
**PLCs:** S7-300, S7-400, S7-1200, S7-1500

### Description
Connects to Siemens S7 PLCs using the S7 protocol over Ethernet.

### Configuration Example
```json
{
  "id": "siemens-plc-1",
  "type": "s7",
  "enabled": true,
  "config": {
    "host": "192.168.1.20",
    "port": 102,
    "rack": 0,
    "slot": 2,
    "timeout": 5000,
    "pollingInterval": 1000,
    "variables": {
      "motor_speed": "DB1,INT0",
      "conveyor_running": "DB1,X2.0",
      "temperature_sp": "DB2,REAL4",
      "production_count": "DB3,DINT10"
    }
  }
}
```

### Variable Addressing
- `DBx,Ty`: Data Block addressing
  - `DB1,INT0`: Integer at byte 0 in DB1
  - `DB1,X2.0`: Bit 0 at byte 2 in DB1
  - `DB2,REAL4`: Real (float) at byte 4 in DB2
  - `DB3,DINT10`: Double integer at byte 10 in DB3

### Supported Data Types
- `X` (Bit), `BYTE`, `WORD`, `DWORD`
- `INT`, `DINT` (Integers)
- `REAL` (Float)
- `STRING`

---

## EtherCAT Connector

**Type:** `ethercat`  
**Library:** `ads-client` (Beckhoff ADS/AMS, TCP 48898)  
**Protocol:** EtherCAT via TwinCAT master

### Description
EtherCAT is a real-time Layer 2 protocol (EtherType 0x88A4) whose cycle is run by an EtherCAT master with raw Ethernet access and real-time scheduling, which a Node.js process cannot provide. The connector therefore talks to the **TwinCAT EtherCAT master** over **ADS**, the standard access path for SCADA/IIoT software:

- **Process data (PDO):** PLC symbols linked to the slaves' I/O (`symbol`), or raw process-image access (`indexGroup`/`indexOffset`/`type`; `0xF020` = inputs, `0xF030` = outputs). All points are read with **one ADS sum command per cycle**.
- **Slave diagnostics:** EtherCAT state (`INIT`/`PREOP`/`SAFEOP`/`OP`, plus `+ERROR`) and link state of every slave, read from the EtherCAT master device (`masterAmsNetId`).
- **CoE mailbox:** periodic SDO reads (`sdo`), plus `readSdo()` / `writeSdo()` on the connector.

Modes: `ads` (default) or `simulation` (random values, no hardware).

### Configuration Example
```json
{
  "id": "ethercat-line-1",
  "type": "ethercat",
  "enabled": true,
  "config": {
    "mode": "ads",
    "targetAmsNetId": "192.168.1.120.1.1",
    "targetAdsPort": 851,
    "masterAmsNetId": "192.168.1.120.3.1",
    "routerAddress": "192.168.1.120",
    "localAmsNetId": "192.168.1.10.1.1",
    "localAdsPort": 32750,
    "pollingInterval": 100,
    "diagnosticsInterval": 1000,
    "slaves": [
      {
        "name": "EL1008",
        "position": 1,
        "inputs": [
          { "name": "sensor_1", "symbol": "GVL_IO.bSensor1" }
        ]
      },
      {
        "name": "EL3202",
        "address": 1003,
        "inputs": [
          { "name": "temperature", "symbol": "GVL_IO.rTemp1" },
          { "name": "raw_ch1", "indexGroup": "0xF020", "indexOffset": 10, "type": "INT" }
        ],
        "outputs": [
          { "name": "valve", "symbol": "GVL_IO.bValve" },
          { "name": "do_bit2", "indexGroup": "0xF030", "indexOffset": 1, "type": "BYTE", "bit": 2 }
        ],
        "sdo": [
          { "name": "vendorId", "index": "0x1018", "subIndex": 1, "type": "UDINT" }
        ]
      }
    ]
  }
}
```

| Field | Description |
|---|---|
| `targetAmsNetId` / `targetAdsPort` | TwinCAT system and PLC runtime port (851 = TC3 PLC1, 801 = TC2) |
| `masterAmsNetId` | AMS Net ID of the EtherCAT master device (TwinCAT → I/O → Device EtherCAT → EtherCAT tab). Optional; enables slave states and SDO |
| `routerAddress`, `routerTcpPort` | ADS router to connect to (default: local router `127.0.0.1:48898`) |
| `localAmsNetId`, `localAdsPort` | Needed when this host has **no TwinCAT router**; add a static route for this host on the TwinCAT system |
| `slaves[].position` / `address` | Bus position (0-based) or EtherCAT fixed address (e.g. 1001). `address` is required for SDO access (resolved from `position` when the master is reachable) |
| `inputs[]` / `outputs[]` | `symbol`, or `indexGroup` + `indexOffset` + `type` (`BOOL, BYTE, USINT, SINT, WORD, UINT, INT, DWORD, UDINT, DINT, REAL, LREAL, LINT, ULINT`), optional `bit` |

If only raw addresses are used, the connector runs as a raw ADS client and does not need a running PLC. After a PLC online change, symbol addresses are resolved again automatically.

---

## PROFINET Connector

**Type:** `profinet`  
**Library:** `nodes7` (S7comm, ISO-on-TCP 102)  
**Protocol:** PROFINET IO via IO Controller

### Description
PROFINET IO cyclic data uses Layer 2 real-time frames (EtherType 0x8892) exchanged between the **IO Controller** and the **IO Devices**. The engineering tool (TIA Portal / STEP 7) maps every IO Device module into the controller's **process image** (I/Q addresses). The connector reads and writes these addresses on the Siemens IO Controller, which gives the live values of each PROFINET device without disturbing the real-time communication.

- Each IO point declares the address assigned in the hardware configuration: `I0.0`, `IB2`, `IW64`, `ID100`, `IR68` (REAL), `Q0.1`, `QW80`, peripheral `PIW256`, as well as `M…` and `DB…` (e.g. `DB10,REAL4`).
- Optional linear `scale` for analog values (e.g. 0..27648 → 0..100 °C).
- Device `status`: `ONLINE`, `ERROR` (a point could not be read), or `OFFLINE`.
- `writeOutput(device, output, value)` writes to the Q area.

Modes: `s7` (default) or `simulation` (random values, no hardware).

**S7-1200/1500:** enable *Permit access with PUT/GET communication* in the CPU protection settings. Typical `slot`: 1 for S7-1200/1500, 2 for S7-300.

### Configuration Example
```json
{
  "id": "profinet-line-1",
  "type": "profinet",
  "enabled": true,
  "config": {
    "mode": "s7",
    "controllerIp": "192.168.0.1",
    "rack": 0,
    "slot": 1,
    "pollingInterval": 100,
    "devices": [
      {
        "name": "ET200SP_1",
        "stationName": "et200sp-1",
        "inputs": [
          { "name": "sensor_1", "address": "I0.0" },
          { "name": "temperature", "address": "IW64",
            "scale": { "rawMin": 0, "rawMax": 27648, "engMin": 0, "engMax": 100 } }
        ],
        "outputs": [
          { "name": "valve_1", "address": "Q0.0" }
        ]
      },
      {
        "name": "G120_Drive",
        "inputs": [
          { "name": "actual_speed", "address": "IW256" }
        ]
      }
    ]
  }
}
```

---

## BACnet Connector

**Type:** `bacnet`  
**Library:** `bacstack`  
**Protocol:** BACnet/IP

### Description
BACnet (Building Automation and Control Networks) is commonly used in HVAC and building automation systems.

### Configuration Example
```json
{
  "id": "bacnet-controller-1",
  "type": "bacnet",
  "enabled": true,
  "config": {
    "port": 47808,
    "interface": "192.168.1.100",
    "broadcastAddress": "192.168.1.255",
    "timeout": 6000,
    "pollingInterval": 5000,
    "devices": [
      {
        "address": "192.168.1.40",
        "deviceId": 1234,
        "objects": [
          {
            "name": "room_temperature",
            "type": 2,
            "instance": 1,
            "property": 85
          },
          {
            "name": "hvac_mode",
            "type": 19,
            "instance": 1,
            "property": 85
          }
        ]
      }
    ]
  }
}
```

### Common Object Types
- `0`: Analog Input
- `1`: Analog Output
- `2`: Analog Value
- `3`: Binary Input
- `4`: Binary Output
- `5`: Binary Value
- `19`: Multi-state Value

### Property ID
- `85`: Present Value (most common)
- `77`: Out of Service
- `111`: Status Flags

---

## FINS (Omron) Connector

**Type:** `fins` or `omron-fins`  
**Protocol:** FINS TCP
**PLCs:** Omron CJ, CS, CP, NJ, NX series

### Description
FINS (Factory Interface Network Service) is Omron's proprietary protocol for PLC communication.

### Configuration Example
```json
{
  "id": "omron-plc-1",
  "type": "fins",
  "enabled": true,
  "config": {
    "host": "192.168.1.50",
    "port": 9600,
    "localNode": 0,
    "remoteNode": 0,
    "localNet": 0,
    "remoteNet": 0,
    "pollingInterval": 1000,
    "memory": [
      {
        "name": "cio_100",
        "area": "CIO",
        "address": 100,
        "length": 1,
        "dataType": "word"
      },
      {
        "name": "dm_1000",
        "area": "DM",
        "address": 1000,
        "length": 2,
        "dataType": "float"
      }
    ]
  }
}
```

### Memory Areas
- `CIO`: Core I/O area
- `WR`: Work area
- `HR`: Holding relay area
- `AR`: Auxiliary relay area
- `DM`: Data memory area
- `EM`: Extended memory area

---

## MELSEC (Mitsubishi) Connector

**Type:** `melsec` or `mitsubishi`  
**Protocol:** MC Protocol (3E Frame)
**PLCs:** Mitsubishi Q, L, FX series

### Description
MC Protocol is Mitsubishi's communication protocol for MELSEC PLCs.

### Configuration Example
```json
{
  "id": "mitsubishi-plc-1",
  "type": "melsec",
  "enabled": true,
  "config": {
    "host": "192.168.1.60",
    "port": 5000,
    "protocol": "3E",
    "networkNo": 0,
    "pcNo": 255,
    "pollingInterval": 1000,
    "devices": [
      {
        "name": "input_x0",
        "deviceCode": "X",
        "address": 0,
        "length": 1,
        "dataType": "bool"
      },
      {
        "name": "output_y10",
        "deviceCode": "Y",
        "address": 10,
        "length": 1,
        "dataType": "bool"
      },
      {
        "name": "data_d100",
        "deviceCode": "D",
        "address": 100,
        "length": 2,
        "dataType": "float"
      }
    ]
  }
}
```

### Device Codes
- `X`: Input
- `Y`: Output
- `M`: Internal relay
- `D`: Data register
- `W`: Link register
- `R`: File register
- `T`: Timer
- `C`: Counter

---

## CIP/EtherNet/IP Connector

**Type:** `cip`, `ethernet-ip`, or `rockwell`  
**Protocol:** Common Industrial Protocol / EtherNet/IP
**PLCs:** Allen-Bradley/Rockwell ControlLogix, CompactLogix, Micro800

### Description
EtherNet/IP is widely used in North America and is the standard protocol for Allen-Bradley PLCs.

**Note:** Requires `ethernet-ip` library: `npm install ethernet-ip`

### Configuration Example
```json
{
  "id": "rockwell-plc-1",
  "type": "cip",
  "enabled": true,
  "config": {
    "host": "192.168.1.70",
    "slot": 0,
    "pollingInterval": 1000,
    "tags": [
      {
        "name": "conveyor_speed",
        "address": "ConveyorSpeed",
        "dataType": "DINT"
      },
      {
        "name": "motor_running",
        "address": "Motor1.Running",
        "dataType": "BOOL"
      },
      {
        "name": "temperature",
        "address": "ProcessTemp",
        "dataType": "REAL"
      }
    ]
  }
}
```

### Data Types
- `BOOL`: Boolean
- `SINT`, `INT`, `DINT`: Signed integers
- `REAL`: Floating point
- `STRING`: String

---

## Serial Connector

**Type:** `serial`, `rs232`, or `rs485`  
**Library:** `serialport`
**Protocols:** Generic serial, RS232, RS485

### Description
Generic serial communication connector for devices using custom protocols, legacy equipment, or standard serial interfaces.

### Configuration Example

#### Active Polling Mode
```json
{
  "id": "serial-device-1",
  "type": "serial",
  "enabled": true,
  "config": {
    "portName": "/dev/ttyUSB0",
    "baudRate": 9600,
    "dataBits": 8,
    "stopBits": 1,
    "parity": "none",
    "mode": "active",
    "pollingInterval": 1000,
    "queryCommand": {
      "hex": "01030000000A"
    },
    "terminator": "0D0A",
    "parser": {
      "type": "ascii"
    }
  }
}
```

#### Passive Listening Mode
```json
{
  "id": "serial-sensor-1",
  "type": "serial",
  "enabled": true,
  "config": {
    "portName": "COM3",
    "baudRate": 115200,
    "dataBits": 8,
    "stopBits": 1,
    "parity": "none",
    "mode": "passive",
    "terminator": "0A",
    "parser": {
      "type": "json"
    }
  }
}
```

### Parser Types
- `ascii`: ASCII string
- `utf8`: UTF-8 string
- `json`: JSON parsing
- `hex`: Hexadecimal string (default)
- `custom`: Custom parser implementation

---

## Installation

Install all connector dependencies:

```bash
npm install
```

Or install specific connector libraries:

```bash
# Modbus
npm install modbus-serial

# Siemens S7
npm install nodes7

# BACnet
npm install bacstack

# Serial
npm install serialport

# EtherNet/IP (optional)
npm install ethernet-ip
```

---

## Protocol Support Matrix

| Protocol | Read | Write | Subscription | Real-time | Library Required |
|----------|------|-------|--------------|-----------|------------------|
| Modbus TCP/RTU | ✅ | ✅ | ❌ | ⚡ Fast | modbus-serial |
| Siemens S7 | ✅ | ✅ | ❌ | ⚡ Fast | nodes7 |
| EtherCAT | ✅ | ✅ | ✅ | ⚡ Fast | Via TwinCAT ADS (`ads-client`) |
| PROFINET | ✅ | ✅ | ✅ | ⚡ Fast | Via IO Controller S7 (`nodes7`) |
| BACnet | ✅ | ✅ | ✅ | 🔄 Medium | bacstack |
| FINS (Omron) | ✅ | ✅ | ❌ | ⚡ Fast | Built-in |
| MELSEC | ✅ | ✅ | ❌ | ⚡ Fast | Built-in |
| EtherNet/IP | ✅ | ✅ | ✅ | ⚡ Fast | ethernet-ip |
| Serial | ✅ | ✅ | ❌ | 🔄 Variable | serialport |

---

## Best Practices

1. **Polling Intervals**: Adjust based on network load and data criticality
   - Fieldbus monitoring: 50-200ms (EtherCAT via ADS, PROFINET via IO Controller; the real-time cycle stays in the master/controller)
   - Fast monitoring: 100-500ms (Modbus, S7)
   - Standard monitoring: 1000-5000ms (BACnet, Serial)

2. **Error Handling**: All connectors implement automatic reconnection with exponential backoff

3. **Data Buffering**: Consider implementing data buffering for critical applications

4. **Network Segmentation**: Use separate networks for real-time protocols (EtherCAT, PROFINET)

5. **Security**: Implement VLANs and firewall rules for industrial networks

---

## Troubleshooting

### Connection Issues
- Verify IP addresses and port numbers
- Check firewall settings
- Ensure PLC/device is in RUN mode
- Verify network connectivity (ping test)

### Data Reading Issues
- Confirm correct addressing format for the protocol
- Check data types match PLC configuration
- Verify register/tag permissions

### Performance Issues
- Reduce polling intervals
- Limit number of registers/tags per request
- Use batch reading where supported

---

## Additional Resources

- [Modbus Protocol Specification](http://www.modbus.org/)
- [Siemens S7 Communication](https://support.industry.siemens.com/)
- [BACnet Standard](http://www.bacnet.org/)
- [ODVA EtherNet/IP](https://www.odva.org/)
- [PROFINET IO](https://www.profibus.com/technology/profinet/)

---

## License

MIT License - See LICENSE file for details
