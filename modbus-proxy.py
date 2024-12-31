#!/usr/bin/env python3
"""
Modbus TCP Proxy Server

This module implements a proxy server for Modbus TCP requests.
It includes functions for loading the configuration, processing requests, and starting the server.
"""

import os
import queue
import threading
import logging
import socket
import ipaddress
from concurrent.futures import ThreadPoolExecutor


def parse_yaml(file_path):
    """
    Parses a simplified YAML configuration file into a dictionary.

    :param file_path: Path to the YAML file
    :return: Parsed dictionary
    """
    config = {}
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):  # Skip empty lines and comments
                continue
            if ":" not in line:
                raise ValueError(f"Invalid YAML syntax in line: {line}")
            
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()

            # Convert values to appropriate types
            if value.isdigit():
                value = int(value)
            elif value.lower() in ["true", "false"]:
                value = value.lower() == "true"
            elif value.startswith("[") and value.endswith("]"):  # Handle lists
                value = [item.strip() for item in value[1:-1].split(",")]

            # Support nested keys using dot notation (e.g., "Proxy.ServerHost")
            keys = key.split(".")
            current = config
            for subkey in keys[:-1]:
                current = current.setdefault(subkey, {})
            current[keys[-1]] = value

    return config


def load_config():
    """
    Loads and validates the configuration file using the custom YAML parser.

    :return: A dictionary containing configuration settings.
    """
    config = parse_yaml("config.yaml")

    # Validate Proxy configuration
    try:
        ipaddress.IPv4Address(config["Proxy"]["ServerHost"])
    except ipaddress.AddressValueError as exc:
        raise ValueError(f"Invalid IPv4 address: {config['Proxy']['ServerHost']}") from exc

    if not 0 < config["Proxy"]["ServerPort"] < 65536:
        raise ValueError(f"Invalid port: {config['Proxy']['ServerPort']}")

    return config


def init_logger(config):
    """
    Initializes the logging system based on the configuration.

    :param config: Configuration settings
    :return: Logger object
    """
    logger = logging.getLogger()
    logger.setLevel(
        getattr(
            logging,
            config["Logging"].get("LogLevel", "INFO").upper(),
            logging.INFO,
        )
    )

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    if config["Logging"].get("Enable", False):
        log_file = config["Logging"].get("LogFile", "modbus_proxy.log")
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def handle_client(client_socket, client_address, request_queue, logger):
    """
    Handles a client connection.

    :param client_socket: Client socket
    :param client_address: Client's address
    :param request_queue: Request queue
    :param logger: Logger object
    """
    try:
        logger.info("New client connected: %s", client_address)
        while True:
            data = client_socket.recv(1024)
            if not data:
                logger.info("Client disconnected: %s", client_address)
                break
            request_queue.put((data, client_socket))
    except (socket.error, OSError) as exc:
        logger.error("Error with client %s: %s", client_address, exc)
    finally:
        client_socket.close()


def process_requests(request_queue, config, logger):
    """
    Processes requests to the Modbus server.

    :param request_queue: Request queue
    :param config: Configuration settings
    :param logger: Logger object
    """
    while True:
        try:
            data, client_socket = request_queue.get()

            # Verbindung zum Modbus-Server herstellen
            with socket.create_connection(
                (config["ModbusServer"]["ModbusServerHost"], int(config["ModbusServer"]["ModbusServerPort"])),
                timeout=5,
            ) as modbus_socket:
                logger.info("Connected to Modbus server")

                # Anfrage an den Modbus-Server senden
                modbus_socket.sendall(data)
                logger.debug("Sent data to Modbus server: %s", data)

                # Antwort vom Modbus-Server empfangen
                response = modbus_socket.recv(1024)
                logger.debug("Received response from Modbus server: %s", response)

                # Antwort an den Client weiterleiten
                client_socket.sendall(response)

        except (socket.error, OSError) as exc:
            logger.error("Error processing request: %s", exc)
            client_socket.close()


def start_server(config):
    """
    Starts the proxy server.

    :param config: Configuration settings
    """
    logger = init_logger(config)

    max_queue_size = max(10, min(1000, threading.active_count() * 10))
    request_queue = queue.Queue(maxsize=max_queue_size)

    cpu_count = os.cpu_count() or 4
    max_workers = max(4, cpu_count * 2)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(
            (config["Proxy"]["ServerHost"], config["Proxy"]["ServerPort"])
        )
        server_socket.listen(5)
        logger.info(
            "Proxy server listening on %s:%d",
            config["Proxy"]["ServerHost"],
            config["Proxy"]["ServerPort"],
        )

        executor.submit(process_requests, request_queue, config, logger)

        try:
            while True:
                client_socket, client_address = server_socket.accept()
                executor.submit(
                    handle_client, client_socket, client_address, request_queue, logger
                )
        except KeyboardInterrupt:
            logger.info("Shutting down server...")
        except OSError as exc:
            logger.error("Server error: %s", exc)
        finally:
            server_socket.close()


if __name__ == "__main__":
    try:
        configuration = load_config()
        start_server(configuration)
    except FileNotFoundError as exc:
        print(f"Configuration file not found: {exc}")
    except ValueError as exc:
        print(f"Invalid configuration: {exc}")
    except OSError as exc:
        print(f"OS error: {exc}")
    except KeyboardInterrupt:
        print("Server shutdown requested by user.")
