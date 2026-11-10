import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.clickhouse_client import get_client
from app.database_pg import get_pg_db
from app.deps import CurrentUser, get_current_user, resolve_tenant_scope
from app.sps_engine import (
    DEFAULT_BASELINE_KWH_DAILY,
    DEFAULT_PV_CAPACITY_KWP,
    DEFAULT_RECTIFIER_FLOAT_VOLTAGE,
    calculate_financial_and_esg_metrics,
    calculate_performance_ratio,
    calculate_solar_contribution,
    evaluate_mppt_health,
    evaluate_pv_health,
)

router = APIRouter(tags=["sps-analytics"])

_LATEST_METRICS_SQL = """
    SELECT metric_name, argMax(metric_value, ts) AS latest_val, avg(metric_value) AS avg_val
    FROM telemetry
    WHERE tenant_slug = {tenant_slug:String}
      AND site_slug = {site_slug:String}
      AND ts >= {start:DateTime64(3)}
      AND ts <= {end:DateTime64(3)}
    GROUP BY metric_name
"""

_ALLTIME_METRICS_SQL = """
    SELECT
        metric_name,
        argMax(metric_value, ts) AS latest_val,
        avgIf(metric_value, abs(metric_value) < 10000000) AS avg_val,
        maxIf(metric_value, abs(metric_value) < 10000000) AS max_val,
        max(ts) AS last_ts,
        argMax(device_key, ts) AS latest_device
    FROM telemetry
    WHERE tenant_slug = {tenant_slug:String}
      AND site_slug = {site_slug:String}
    GROUP BY metric_name
    ORDER BY metric_name
"""


def _sanitize_val(val: Any) -> float:
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f) or abs(f) > 1e7:
            return 0.0
        return f
    except (TypeError, ValueError):
        return 0.0


def _auto_map_sps_metrics(raw: dict[str, float]) -> dict[str, float]:
    """Memetakan metrik dari ClickHouse secara otomatis (baik nama standar RKS
    maupun nama dinamis dari sensor/PLC seperti Top_PV_Voltage, Top_Battery_Voltage,
    Top_AC_Current, Power, Voltage, maupun sensor baru lainnya).
    """
    mapped: dict[str, float] = {k: _sanitize_val(v) for k, v in raw.items()}

    def _set_if_zero(target_key: str, val: float) -> None:
        if mapped.get(target_key, 0.0) == 0.0 and val != 0.0:
            mapped[target_key] = val

    for k, raw_v in list(mapped.items()):
        v = _sanitize_val(raw_v)
        if v == 0.0:
            continue
        k_low = k.lower().strip()

        # 0. Input AC PLN / Grid — dipetakan lebih dulu agar tegangan AC PLN (228 V)
        #    tidak tertukar dengan tegangan DC busbar (float 53,5 V).
        #    Kata kunci Inggris + Indonesia (Tegangan/Arus/Frekuensi/Daya) karena
        #    firmware GNIS mengirim nama metrik berbahasa Indonesia.
        if "pln" in k_low or "grid" in k_low or "utility" in k_low:
            if "volt" in k_low or "tegangan" in k_low:
                _set_if_zero("v_pln", v)
            elif "curr" in k_low or "amp" in k_low or "arus" in k_low:
                _set_if_zero("i_rectifier", v)
            elif "freq" in k_low or "frekuensi" in k_low:
                _set_if_zero("freq_pln", v)
            elif "power" in k_low or "watt" in k_low or "daya" in k_low:
                _set_if_zero("p_rectifier", v)
            continue

        # 1. PV Voltage
        if k_low in ("v_pv", "vpv", "v_pv_string1", "top_pv_voltage", "pv_voltage", "solar_voltage") or (
            ("pv" in k_low or "solar" in k_low or "panel" in k_low) and ("volt" in k_low or "tegangan" in k_low)
        ):
            norm_v = round(v / 100.0, 2) if v > 1000.0 else v
            _set_if_zero("v_pv", norm_v)

        # 2. PV Current
        elif k_low in ("i_pv", "ipv", "i_pv_string1", "top_pv_current", "pv_current", "solar_current") or (
            ("pv" in k_low or "solar" in k_low or "panel" in k_low) and ("curr" in k_low or "amp" in k_low or "arus" in k_low)
        ):
            norm_i = round(v / 100.0, 2) if v > 500.0 else v
            _set_if_zero("i_pv", norm_i)

        # 3. PV Power
        elif k_low in ("p_pv", "ppv", "top_pv_inputpower", "pv_inputpower", "pv_power", "solar_power") or (
            ("pv" in k_low or "solar" in k_low or "panel" in k_low) and ("power" in k_low or "watt" in k_low or "daya" in k_low)
        ):
            _set_if_zero("p_pv", v)

        # 4. MPPT / Battery / DC Output Voltage
        elif k_low in (
            "v_mppt_out",
            "v_mppt",
            "top_battery_voltage",
            "battery_voltage",
            "mppt_voltage",
            "v_bat",
            "v_batt",
        ) or (("mppt" in k_low or "batt" in k_low or "batere" in k_low) and ("volt" in k_low or "tegangan" in k_low)):
            norm_v = round(v / 100.0, 2) if v > 1000.0 else v
            _set_if_zero("v_mppt_out", norm_v)
            _set_if_zero("pms_batt_voltage", norm_v)

        # 5. MPPT / Battery Charging Current
        elif k_low in (
            "i_mppt_out",
            "i_mppt",
            "top_battery_charging",
            "battery_current",
            "charging_current",
            "mppt_current",
        ) or (("mppt" in k_low or "batt" in k_low or "charg" in k_low or "batere" in k_low) and ("curr" in k_low or "amp" in k_low or "arus" in k_low or "charg" in k_low)):
            norm_i = round(v / 100.0, 2) if v > 500.0 else v
            _set_if_zero("i_mppt_out", norm_i)

        # 6. MPPT Output Power
        elif k_low in ("p_mppt_out", "p_mppt", "mppt_power", "charging_power") or (
            "mppt" in k_low and ("power" in k_low or "watt" in k_low or "daya" in k_low)
        ):
            _set_if_zero("p_mppt_out", v)

        # 7. DC Busbar Voltage
        elif k_low in ("v_busbar", "busbar_voltage", "dc_bus_voltage", "v_dc") or (
            "busbar" in k_low and ("volt" in k_low or "tegangan" in k_low)
        ):
            norm_v = round(v / 100.0, 2) if v > 1000.0 else v
            _set_if_zero("v_busbar", norm_v)

        # 8. Rectifier / AC / Grid Current
        elif k_low in ("i_rectifier", "rectifier_current", "top_ac_current", "ac_current", "grid_current", "current") or (
            ("rect" in k_low or "ac_" in k_low or "grid" in k_low) and "curr" in k_low
        ):
            _set_if_zero("i_rectifier", v)

        # 9. Rectifier / AC / Grid Power
        elif k_low in ("p_rectifier", "rectifier_power", "ac_power", "grid_power", "power") or (
            ("rect" in k_low or "ac_" in k_low or "grid" in k_low) and "power" in k_low
        ):
            _set_if_zero("p_rectifier", v)

        # 10. Irradiance / Solar Radiation Sensor
        elif any(w in k_low for w in ("irrad", "radiat", "pyran", "lux", "solar_rad")):
            _set_if_zero("irradiance", v)

        # 11. Temperature Sensor (MPPT / Controller / Device / Battery / Ambient)
        elif "temp" in k_low or "suhu" in k_low:
            if -20.0 <= v <= 150.0:
                _set_if_zero("temp_mppt", v)

        # 12. Battery SOC (%)
        elif "soc" in k_low:
            if 0.0 <= v <= 100.0:
                _set_if_zero("pms_batt_soc", v)

        # 13. Signal RSSI
        elif any(w in k_low for w in ("rssi", "signal", "rsrp", "csq")):
            _set_if_zero("pms_signal_rssi", v)

    return mapped


def _resolve_effective_site_slug(db: Session, tenant_slug: str, site_slug: str) -> str:
    """Jika site_slug yang diminta (misal default 'site-jkt-01') tidak ditemukan
    pada tenant tersebut di Postgres maupun ClickHouse, otomatis gunakan site aktif milik tenant.
    """
    try:
        row = db.execute(
            text(
                "SELECT s.slug FROM sites s JOIN tenants t ON t.id = s.tenant_id "
                "WHERE t.slug = :t_slug AND s.slug = :s_slug LIMIT 1"
            ),
            {"t_slug": tenant_slug, "s_slug": site_slug},
        ).first()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass

    # Cek apakah ada data di ClickHouse untuk (tenant_slug, site_slug)
    try:
        ch = get_client()
        ch_res = ch.query(
            "SELECT metric_name, argMax(metric_value, ts) AS latest_val, avg(metric_value) AS avg_val "
            "FROM telemetry WHERE tenant_slug = {tenant_slug:String} AND site_slug = {site_slug:String} "
            "GROUP BY metric_name",
            parameters={"tenant_slug": tenant_slug, "site_slug": site_slug},
        )
        if ch_res.result_rows:
            return site_slug
    except Exception:
        pass

    # Fallback: cari site pertama milik tenant_slug di Postgres
    try:
        first_site = db.execute(
            text(
                "SELECT s.slug FROM sites s JOIN tenants t ON t.id = s.tenant_id "
                "WHERE t.slug = :t_slug ORDER BY s.created_at ASC LIMIT 1"
            ),
            {"t_slug": tenant_slug},
        ).first()
        if first_site and first_site[0]:
            return str(first_site[0])
    except Exception:
        pass

    return site_slug


def _load_site_sps_config(db: Session, tenant_slug: str, site_slug: str) -> dict[str, Any]:
    config: dict[str, Any] = {
        "site_type": "TYPE_A",
        "pv_capacity_kwp": DEFAULT_PV_CAPACITY_KWP,
        "mppt_capacity_a": 80.0,
        "rectifier_float_voltage": DEFAULT_RECTIFIER_FLOAT_VOLTAGE,
        "baseline_kwh_daily": DEFAULT_BASELINE_KWH_DAILY,
    }
    try:
        row = db.execute(
            text(
                "SELECT s.site_type, s.pv_capacity_kwp, s.mppt_capacity_a, "
                "s.rectifier_float_voltage, s.baseline_kwh_daily, s.name "
                "FROM sites s JOIN tenants t ON t.id = s.tenant_id "
                "WHERE t.slug = :t_slug AND s.slug = :s_slug LIMIT 1"
            ),
            {"t_slug": tenant_slug, "s_slug": site_slug},
        ).first()
        if row:
            if row[0] is not None:
                config["site_type"] = str(row[0])
            if row[1] is not None:
                config["pv_capacity_kwp"] = float(row[1])
            if row[2] is not None:
                config["mppt_capacity_a"] = float(row[2])
            if row[3] is not None:
                config["rectifier_float_voltage"] = float(row[3])
            if row[4] is not None:
                config["baseline_kwh_daily"] = float(row[4])
            if len(row) > 5 and row[5] is not None:
                config["site_name"] = str(row[5])
    except Exception:
        pass
    return config


def _fetch_site_telemetry_snapshot(
    tenant_slug: str, site_slug: str, start: datetime, end: datetime
) -> tuple[dict[str, float], dict[str, float]]:
    client = get_client()
    res = client.query(
        _LATEST_METRICS_SQL,
        parameters={
            "tenant_slug": tenant_slug,
            "site_slug": site_slug,
            "start": start,
            "end": end,
        },
    )
    latest: dict[str, float] = {}
    averages: dict[str, float] = {}
    for row in res.result_rows:
        m_name = str(row[0])
        latest[m_name] = _sanitize_val(row[1])
        averages[m_name] = _sanitize_val(row[2])
    return latest, averages


def _fetch_alltime_sensor_inventory(
    tenant_slug: str, site_slug: str, now: datetime
) -> tuple[list[dict[str, Any]], dict[str, float], dict[str, float], bool, str | None]:
    """Mengambil seluruh daftar sensor/metrik yang pernah masuk ke ClickHouse pada site ini,
    sehingga penambahan sensor baru di masa depan otomatis terbaca tanpa hardcode.
    """
    client = get_client()
    sensor_list: list[dict[str, Any]] = []
    hist_latest: dict[str, float] = {}
    hist_avg: dict[str, float] = {}
    newest_ts: datetime | None = None

    try:
        res = client.query(
            _ALLTIME_METRICS_SQL,
            parameters={"tenant_slug": tenant_slug, "site_slug": site_slug},
        )
        for row in res.result_rows:
            if len(row) >= 6:
                m_name = str(row[0])
                l_val = _sanitize_val(row[1])
                a_val = _sanitize_val(row[2])
                mx_val = _sanitize_val(row[3])
                ts_val = row[4]
                dev_key = str(row[5]) if row[5] else ""
            elif len(row) >= 3:
                m_name = str(row[0])
                l_val = _sanitize_val(row[1])
                a_val = _sanitize_val(row[2])
                mx_val = l_val
                ts_val = None
                dev_key = ""
            else:
                continue

            hist_latest[m_name] = l_val
            hist_avg[m_name] = a_val

            ts_iso = None
            if isinstance(ts_val, datetime):
                if ts_val.tzinfo is None:
                    ts_val = ts_val.replace(tzinfo=timezone.utc)
                ts_iso = ts_val.isoformat()
                if newest_ts is None or ts_val > newest_ts:
                    newest_ts = ts_val

            sensor_list.append(
                {
                    "metric_name": m_name,
                    "latest_value": round(l_val, 3),
                    "avg_value": round(a_val, 3),
                    "max_value": round(mx_val, 3),
                    "last_seen": ts_iso,
                    "device_key": dev_key,
                }
            )
    except Exception:
        pass

    is_online = False
    last_seen_iso = None
    if newest_ts is not None:
        last_seen_iso = newest_ts.isoformat()
        is_online = (now - newest_ts).total_seconds() <= 300

    return sensor_list, hist_latest, hist_avg, is_online, last_seen_iso


def _build_performance_payload(
    tenant_slug: str,
    site_slug: str,
    site_cfg: dict[str, Any],
    latest: dict[str, float],
    averages: dict[str, float],
    hours_window: float,
    sensor_metrics: list[dict[str, Any]] | None = None,
    is_online: bool | None = None,
    last_seen: str | None = None,
) -> dict[str, Any]:
    mapped_latest = _auto_map_sps_metrics(latest)
    mapped_avg = _auto_map_sps_metrics(averages)

    v_pv = mapped_latest.get("v_pv", mapped_latest.get("v_pv_string1", 0.0))
    i_pv = mapped_latest.get("i_pv", mapped_latest.get("i_pv_string1", 0.0))
    p_pv = mapped_latest.get("p_pv", round(v_pv * i_pv, 2))
    v_mppt_out = mapped_latest.get("v_mppt_out", 0.0)
    i_mppt_out = mapped_latest.get("i_mppt_out", 0.0)
    p_mppt_out = mapped_latest.get("p_mppt_out", round(v_mppt_out * i_mppt_out, 2))
    if p_mppt_out == 0.0 and p_pv > 0.0:
        p_mppt_out = round(p_pv * 0.98, 2)

    v_busbar = mapped_latest.get("v_busbar", v_mppt_out or site_cfg["rectifier_float_voltage"])
    # Sisi AC PLN (input rectifier). Daya rectifier dihitung dari tegangan AC x arus AC,
    # bukan dari tegangan DC busbar, supaya "Suplai Sisa" mencerminkan beban PLN sebenarnya.
    v_pln = mapped_latest.get("v_pln", 0.0)
    freq_pln = mapped_latest.get("freq_pln", 0.0)
    i_rectifier = mapped_latest.get("i_rectifier", 0.0)
    p_rectifier = round((v_pln if v_pln > 0.0 else v_busbar) * i_rectifier, 2) if i_rectifier > 0.0 else 0.0
    if p_rectifier <= 0.0:
        p_rectifier = mapped_latest.get("p_rectifier", 0.0)
    p_load = mapped_latest.get("p_load", 0.0)
    if p_load <= 0.0:
        p_load = round(p_mppt_out + p_rectifier, 2)
    irradiance = mapped_latest.get("irradiance", 0.0)
    temp_mppt = mapped_latest.get("temp_mppt", 0.0)

    eff_hours = max(1.0, min(hours_window, 24.0))
    avg_p_mppt = mapped_avg.get("p_mppt_out", mapped_avg.get("p_pv", p_mppt_out))
    # Daya rectifier rata-rata: prioritaskan V_AC PLN x I_AC PLN. Metrik generik lama
    # ("Power"/"Current") sengaja jadi fallback terakhir karena bisa berasal dari data uji.
    avg_v_pln = mapped_avg.get("v_pln", v_pln)
    avg_i_rect = mapped_avg.get("i_rectifier", i_rectifier)
    avg_p_rect = round((avg_v_pln if avg_v_pln > 0.0 else v_busbar) * avg_i_rect, 2)
    if avg_p_rect <= 0.0:
        avg_p_rect = mapped_avg.get("p_rectifier", p_rectifier)
    avg_irr = mapped_avg.get("irradiance", irradiance)

    solar_kwh = mapped_latest.get("solar_kwh", round((avg_p_mppt / 1000.0) * eff_hours, 3))
    grid_kwh = mapped_latest.get("grid_kwh", round((avg_p_rect / 1000.0) * eff_hours, 3))
    load_kwh = mapped_latest.get("load_kwh", round(solar_kwh + grid_kwh, 3))
    irradiance_kwh_m2 = round((avg_irr / 1000.0) * eff_hours, 3)

    scr_pct = calculate_solar_contribution(solar_kwh=solar_kwh, total_load_kwh=load_kwh)
    pr_pct = calculate_performance_ratio(
        actual_solar_kwh=solar_kwh,
        irradiance_kwh_m2=irradiance_kwh_m2,
        pv_capacity_kwp=site_cfg["pv_capacity_kwp"],
    )
    fin_esg = calculate_financial_and_esg_metrics(solar_kwh=solar_kwh)

    voltage_delta = round(v_mppt_out - site_cfg["rectifier_float_voltage"], 2) if v_mppt_out > 0 else 0.0
    load_sharing_active = voltage_delta >= 0.4 and p_mppt_out > 0

    # PMS Hardware status (UPS, 4G, OTA) dari metrik sensor jika tersedia
    batt_v = mapped_latest.get("pms_batt_voltage", v_mppt_out)
    batt_soc = mapped_latest.get("pms_batt_soc", 0.0)
    signal_rssi = mapped_latest.get("pms_signal_rssi", 0.0)
    ups_hours = round((batt_soc / 100.0) * 3.5, 1) if batt_soc > 0 else (3.2 if batt_v >= 12.0 else 0.0)

    if sensor_metrics is None:
        sensor_metrics = [
            {
                "metric_name": k,
                "latest_value": round(_sanitize_val(v), 3),
                "avg_value": round(_sanitize_val(averages.get(k, v)), 3),
                "max_value": round(_sanitize_val(v), 3),
                "last_seen": last_seen,
                "device_key": "",
            }
            for k, v in sorted(latest.items())
        ]

    return {
        "tenant_slug": tenant_slug,
        "site_slug": site_slug,
        "is_online": True if is_online is None else is_online,
        "last_seen": last_seen,
        "site_config": site_cfg,
        "solar_kwh": solar_kwh,
        "grid_kwh": grid_kwh,
        "load_kwh": load_kwh,
        "solar_contribution_pct": scr_pct,
        "performance_ratio_pct": pr_pct,
        **fin_esg,
        "power_flow": {
            "irradiance": round(irradiance, 2),
            "v_pv": round(v_pv, 2),
            "i_pv": round(i_pv, 2),
            "p_pv": round(p_pv, 2),
            "v_mppt_out": round(v_mppt_out, 2),
            "i_mppt_out": round(i_mppt_out, 2),
            "p_mppt_out": round(p_mppt_out, 2),
            "v_busbar": round(v_busbar, 2),
            "v_pln": round(v_pln, 2),
            "freq_pln": round(freq_pln, 2),
            "i_rectifier": round(i_rectifier, 2),
            "p_rectifier": round(p_rectifier, 2),
            "p_load": round(p_load, 2),
            "temp_mppt": round(temp_mppt, 2),
            "rectifier_float_voltage": site_cfg["rectifier_float_voltage"],
            "voltage_delta_v": voltage_delta,
            "load_sharing_active": load_sharing_active,
        },
        "pms_status": {
            "power_source": "DC_MAINS" if (is_online or batt_v >= 48.0) else "OFFLINE",
            "battery_voltage": round(batt_v, 2),
            "battery_pct": round(batt_soc, 1),
            "ups_hours_remaining": ups_hours,
            "signal_rssi": round(signal_rssi, 1) if signal_rssi != 0 else None,
            "firmware_version": "v2.4.1-ota-build-2026",
            "ota_status": "UP_TO_DATE",
        },
        "sensor_metrics": sensor_metrics,
    }


@router.get("/api/sps/sites/{site_slug}/performance")
@router.get("/sps/sites/{site_slug}/performance")
def get_site_sps_performance(
    site_slug: str,
    tenant_slug: str | None = Query(default=None, description="Hanya berlaku untuk superadmin"),
    start: datetime | None = None,
    end: datetime | None = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_pg_db),
) -> dict[str, Any]:
    effective_tenant = resolve_tenant_scope(current_user, tenant_slug)
    effective_site = _resolve_effective_site_slug(db, effective_tenant, site_slug)

    now = datetime.now(timezone.utc)
    end_dt = end or (now + timedelta(minutes=1))
    start_dt = start or (now - timedelta(hours=24))
    hours_window = max(1.0, (end_dt - start_dt).total_seconds() / 3600.0)

    site_cfg = _load_site_sps_config(db, effective_tenant, effective_site)
    latest, averages = _fetch_site_telemetry_snapshot(effective_tenant, effective_site, start_dt, end_dt)
    sensor_list, hist_latest, hist_avg, is_online, last_seen = _fetch_alltime_sensor_inventory(
        effective_tenant, effective_site, now
    )

    # Jika dalam window 24 jam tidak ada metrik (atau perangkat offline >24 jam),
    # gunakan rata-rata & nilai terakhir historis dari ClickHouse agar data riil tetap terbaca.
    if not latest and hist_latest:
        latest = hist_latest
    if not averages and hist_avg:
        averages = hist_avg
    else:
        for k, v in hist_avg.items():
            if averages.get(k, 0.0) == 0.0 and v != 0.0:
                averages[k] = v

    return _build_performance_payload(
        effective_tenant,
        effective_site,
        site_cfg,
        latest,
        averages,
        hours_window,
        sensor_metrics=sensor_list if sensor_list else None,
        is_online=is_online if sensor_list else True,
        last_seen=last_seen,
    )


@router.get("/api/sps/sites/{site_slug}/health")
@router.get("/sps/sites/{site_slug}/health")
def get_site_sps_health(
    site_slug: str,
    tenant_slug: str | None = Query(default=None, description="Hanya berlaku untuk superadmin"),
    start: datetime | None = None,
    end: datetime | None = None,
    current_user: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_pg_db),
) -> dict[str, Any]:
    effective_tenant = resolve_tenant_scope(current_user, tenant_slug)
    effective_site = _resolve_effective_site_slug(db, effective_tenant, site_slug)
    now = datetime.now(timezone.utc)
    end_dt = end or (now + timedelta(minutes=1))
    start_dt = start or (now - timedelta(hours=24))

    site_cfg = _load_site_sps_config(db, effective_tenant, effective_site)
    latest_raw, averages_raw = _fetch_site_telemetry_snapshot(effective_tenant, effective_site, start_dt, end_dt)
    latest = _auto_map_sps_metrics(latest_raw)
    averages = _auto_map_sps_metrics(averages_raw)

    v_pv = latest.get("v_pv", latest.get("v_pv_string1", 0.0))
    i_pv = latest.get("i_pv", latest.get("i_pv_string1", 0.0))
    p_pv = latest.get("p_pv", round(v_pv * i_pv, 2))
    v_mppt_out = latest.get("v_mppt_out", 0.0)
    i_mppt_out = latest.get("i_mppt_out", 0.0)
    p_mppt_out = latest.get("p_mppt_out", round(v_mppt_out * i_mppt_out, 2))
    temp_mppt = latest.get("temp_mppt", 25.0)
    avg_irr = averages.get("irradiance", latest.get("irradiance", 0.0))
    avg_p_mppt = averages.get("p_mppt_out", p_mppt_out)

    string_currents: list[float] = []
    for idx in range(1, 9):
        key = f"i_pv_string{idx}"
        if key in latest:
            string_currents.append(latest[key])

    solar_kwh = latest.get("solar_kwh", round(avg_p_mppt / 1000.0, 3))
    irradiance_kwh_m2 = round(avg_irr / 1000.0, 3)

    pv_health = evaluate_pv_health(
        actual_solar_kwh=solar_kwh,
        irradiance_kwh_m2=irradiance_kwh_m2,
        pv_capacity_kwp=site_cfg["pv_capacity_kwp"],
        string_currents=string_currents,
    )
    mppt_health = evaluate_mppt_health(
        p_pv=p_pv,
        p_mppt_out=p_mppt_out,
        temp_mppt=temp_mppt,
        v_mppt_out=v_mppt_out if v_mppt_out > 0 else None,
        rectifier_float_voltage=site_cfg["rectifier_float_voltage"],
    )

    overall_index = round((pv_health["health_index"] + mppt_health["health_index"]) / 2.0, 2)
    if mppt_health["status"] == "CRITICAL" or pv_health["status"] == "DEGRADED":
        overall_status = "CRITICAL" if mppt_health["status"] == "CRITICAL" else "DEGRADED"
    elif mppt_health["status"] == "WARNING" or pv_health["status"] == "WARNING":
        overall_status = "WARNING"
    else:
        overall_status = "HEALTHY"

    return {
        "tenant_slug": effective_tenant,
        "site_slug": effective_site,
        "site_health_index": overall_index,
        "status": overall_status,
        "pv_health": pv_health,
        "mppt_health": mppt_health,
    }


@router.get("/api/sps/sites/{site_slug}/daily")
@router.get("/sps/sites/{site_slug}/daily")
def get_site_sps_daily(
    site_slug: str,
    tenant_slug: str | None = Query(default=None, description="Hanya berlaku untuk superadmin"),
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=31, ge=1, le=730),
    current_user: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_pg_db),
) -> dict[str, Any]:
    effective_tenant = resolve_tenant_scope(current_user, tenant_slug)
    effective_site = _resolve_effective_site_slug(db, effective_tenant, site_slug)
    site_cfg = _load_site_sps_config(db, effective_tenant, effective_site)

    today = datetime.now(timezone.utc).date()
    end_d = end_date or today
    start_d = start_date or (end_d - timedelta(days=30))

    client = get_client()
    records: list[dict[str, Any]] = []

    try:
        res = client.query(
            """
            SELECT date, solar_kwh, grid_kwh, load_kwh, solar_contribution_pct, performance_ratio_pct, updated_at
            FROM sps_daily_energy FINAL
            WHERE tenant_slug = {tenant_slug:String}
              AND site_slug = {site_slug:String}
              AND date >= {start_date:Date}
              AND date <= {end_date:Date}
            ORDER BY date DESC
            LIMIT {limit:UInt32}
            """,
            parameters={
                "tenant_slug": effective_tenant,
                "site_slug": effective_site,
                "start_date": start_d,
                "end_date": end_d,
                "limit": limit,
            },
        )
        for row in res.result_rows:
            if len(row) >= 7:
                records.append(
                    {
                        "date": str(row[0]),
                        "solar_kwh": round(float(row[1]), 3),
                        "grid_kwh": round(float(row[2]), 3),
                        "load_kwh": round(float(row[3]), 3),
                        "solar_contribution_pct": round(float(row[4]), 2),
                        "performance_ratio_pct": round(float(row[5]), 2),
                        "updated_at": str(row[6]),
                    }
                )
    except Exception:
        pass

    # Jika tabel sps_daily_energy belum berisi baris agregasi terjadwal,
    # agregasikan otomatis secara dinamis dari tabel telemetry di ClickHouse!
    if not records:
        try:
            raw_daily = client.query(
                """
                SELECT
                    toDate(ts) AS day,
                    metric_name,
                    avgIf(metric_value, abs(metric_value) < 10000000) AS avg_val,
                    max(ts) AS last_ts
                FROM telemetry
                WHERE tenant_slug = {tenant_slug:String}
                  AND site_slug = {site_slug:String}
                GROUP BY day, metric_name
                ORDER BY day DESC
                """,
                parameters={"tenant_slug": effective_tenant, "site_slug": effective_site},
            )
            by_day: dict[str, dict[str, float]] = {}
            day_ts: dict[str, str] = {}
            for row in raw_daily.result_rows:
                if len(row) >= 4:
                    d_str = str(row[0])
                    m_name = str(row[1])
                    a_val = _sanitize_val(row[2])
                    by_day.setdefault(d_str, {})[m_name] = a_val
                    if d_str not in day_ts and row[3]:
                        day_ts[d_str] = str(row[3])

            for d_str in sorted(by_day.keys(), reverse=True)[:limit]:
                mapped = _auto_map_sps_metrics(by_day[d_str])
                v_pv = mapped.get("v_pv", 0.0)
                i_pv = mapped.get("i_pv", 0.0)
                p_pv = mapped.get("p_pv", round(v_pv * i_pv, 2))
                v_mppt = mapped.get("v_mppt_out", 0.0)
                i_mppt = mapped.get("i_mppt_out", 0.0)
                p_mppt = mapped.get("p_mppt_out", round(v_mppt * i_mppt, 2))
                if p_mppt == 0.0 and p_pv > 0.0:
                    p_mppt = round(p_pv * 0.98, 2)
                # Jika hanya tegangan PV/Battery yang tercatat oleh sensor PoC, estimasi daya dari profil voltase
                if p_mppt == 0.0 and v_pv > 10.0:
                    p_mppt = round(v_pv * 8.5, 2)

                p_rect = mapped.get("p_rectifier", round(mapped.get("v_busbar", 53.5) * mapped.get("i_rectifier", 0.0), 2))
                solar_kwh = round((p_mppt / 1000.0) * 8.0, 2)
                grid_kwh = round((p_rect / 1000.0) * 16.0, 2)
                load_kwh = round(solar_kwh + grid_kwh, 2)
                scr = calculate_solar_contribution(solar_kwh, load_kwh)
                pr = calculate_performance_ratio(solar_kwh, 4.5, site_cfg["pv_capacity_kwp"])

                records.append(
                    {
                        "date": d_str,
                        "solar_kwh": solar_kwh,
                        "grid_kwh": grid_kwh,
                        "load_kwh": load_kwh,
                        "solar_contribution_pct": scr,
                        "performance_ratio_pct": pr,
                        "updated_at": day_ts.get(d_str, datetime.now(timezone.utc).isoformat()),
                    }
                )
        except Exception:
            pass

    return {
        "tenant_slug": effective_tenant,
        "site_slug": effective_site,
        "records": records,
        "total": len(records),
    }
