#!/usr/bin/env python3
"""Validate CRUSH security contract syntax without altering Wazuh native decoder layout."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RULES = ROOT / "deploy/security/wazuh/rules/110500-crush-healthcare-security.xml"
DECODERS = ROOT / "deploy/security/wazuh/decoders/110500-crush-healthcare-security.xml"
REQUIRED_RULE_IDS = {str(rule_id) for rule_id in range(110500, 110511)}


def parse_rules() -> None:
    root = ET.parse(RULES).getroot()
    observed = {element.attrib.get("id") for element in root.findall("rule")}
    missing = REQUIRED_RULE_IDS - observed
    if missing:
        raise ValueError(f"missing expected Wazuh rule IDs: {sorted(missing)}")


def parse_decoders() -> None:
    # Wazuh accepts sequential <decoder> elements in one custom decoder file.
    # Wrap only for standard XML parser validation; do not write this wrapper back.
    source = DECODERS.read_text(encoding="utf-8")
    root = ET.fromstring(f"<decoders>{source}</decoders>")
    names = {element.attrib.get("name") for element in root.findall("decoder")}
    required = {"crush-healthcare-security-json", "crush-healthcare-security-privacy-guard"}
    missing = required - names
    if missing:
        raise ValueError(f"missing expected Wazuh decoder names: {sorted(missing)}")


def validate_mapping() -> None:
    mapping = ROOT / "deploy/security/opencti-stix-wazuh-mapping.yaml"
    content = mapping.read_text(encoding="utf-8")
    for token in ("stixSpec: \"2.1\"", "observed-data", "sighting", "relationship", "requiredDropFields"):
        if token not in content:
            raise ValueError(f"STIX mapping missing required token: {token}")


def main() -> None:
    parse_rules()
    parse_decoders()
    validate_mapping()
    print("Security contract validation passed")


if __name__ == "__main__":
    main()
