## How to Run

### Terminal 1
```bash
python main.py --site-id A --port 9001 --peers 127.0.0.1:9002,127.0.0.1:9003
```

### Terminal 2
```bash
python main.py --site-id B --port 9002 --peers 127.0.0.1:9001,127.0.0.1:9003
```

### Terminal 3
```bash
python main.py --site-id C --port 9003 --peers 127.0.0.1:9001,127.0.0.1:9002
```

## How to Use (WIP)

```bash
insert <position> <character> # Insert character at position
```

```bash
delete <position> # Delete character at position
```

```bash
show # Show current document state
```

```bash
peers # Show connected peers
```

```bash
exit # Exit the program
```
