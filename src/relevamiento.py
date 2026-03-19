import os, json, time
from datetime import datetime, date, timedelta
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

AHORA_AR  = datetime.utcnow() - timedelta(hours=3)
HOY       = AHORA_AR.date()
PROX_DIAS = 30

NOMBRE_A_CODIGO = {
    "HECHO RELEVANTE": "MUG_001",
    "HECHO RELEVANTE - MIGRACION": "MUG_001",
    "DATOS BASICOS DEL ADMINISTRADO": "MUG_002",
    "DOMICILIO ELECTRONICO": "MUG_004",
    "ORGANIGRAMA": "MUG_005",
    "ORGANIGRAMA - MIGRACION": "MUG_005",
    "COMPOSICION DEL CAPITAL Y TENENCIAS": "MUG_008",
    "ACTA DE ASAMBLEA Y/O REUNIÓN DE SOCIOS03": "MUG_021",
    "ACTA DE ASAMBLEA Y/O REUNIÓN DE SOCIOS": "MUG_021",
    "ACTA DE ASAMBLEA - MIGRACION": "MUG_021",
    "ACTA DE ÓRGANO DE ADMINISTRACIÓN (DIRECTORIO)": "MUG_022",
    "ACTAS DEL ÓRGANO DE ADMINISTRACIÓN - MIGRACION": "MUG_022",
    "CONVOCATORIA A ASAMBLEA (ORDEN DEL DÍA) - MIGRACION": "MUG_025",
    "ESTATUTOS VIGENTES": "MUG_028",
    "ESTATUTO VIGENTE": "MUG_028",
    "ESTATUTO VIGENTE ORDENADO - MIGRACION": "MUG_028",
    "NOMINA DE AUDITORES EXTERNOS": "MUG_011",
    "NÓMINA DE AUDITORES EXTERNOS": "MUG_011",
    "AUDITORES EXTERNOS - MIGRACION": "MUG_011",
    "NÓMINA DE DIRECTORES": "MUG_013",
    "NOMINA MIEMBROS ORGANO ADM.- FISCALIZACIÓN- GERENTES ART.270 - APODERADOS - MIGRACION": "MUG_013",
    "NÓMINA DE FAMILIARES DE MIEMBROS DE ÓRGANOS DE ADMINISTRACIÓN Y FISCALIZACIÓN": "MUG_014",
    "NÓMINA DE SÍNDICO O COMISION FISCALIZADORA": "MUG_016",
    "NÓMINA DE SÍNDICOS O COMISIÓN FISCALIZADORA": "MUG_016",
    "MEMBRESIAS": "AGE_002",
    "MEMBRESÍAS": "AGE_002",
    "MERCADO DONDE SON MIEMBROS - MIGRACION": "AGE_002",
    "DESIGNACIÓN DE RESPONSABLE DE CUMPLIMIENTO REGULATORIO Y CONTROL INTERNO": "AGE_003",
    "DESIGNACIÓN RESPONSABLE DE CUMPLIMIENTO REGULATORIO Y CONTROL INTERNO": "AGE_003",
    "RESPONSABLE CUMPLIMENTO REGULATORIO - MIGRACION": "AGE_003",
    "RESPONSABLE RELACIONES CON EL PÚBLICO - MIGRACION": "AGE_004",
    "DESIGNACIÓN RESPONSABLE DE RELACIONES CON EL PÚBLICO": "AGE_004",
    "VALORIZACIÓN DE CARTERAS ADMINISTRADAS.": "AGE_007",
    "ADELANTOS TRANSITORIOS OTORGADOS- INC. B) ART. 11, CAPÍTULO VII": "AGE_008",
    "PROCEDIMIENTO PARA SEGREGACIÓN DE ACTIVOS": "AGE_010",
    "PROCEDIMIENTO SEGREGACIÓN DE ACTIVOS": "AGE_010",
    "PROCEDIMIENTO PARA SEPARACIÓN DE ACTIVOS-INFORMACIÓN DE CUENTAS - MIGRACION": "AGE_010",
    "INFORME AUDITORÍA ANUAL DE SISTEMAS": "AGE_012",
    "INFORME AUDITORÍA SISTEMAS (ANUAL) - MIGRACION": "AGE_012",
    "INFORME PERIÓDICO DE RESPONSABLE DE CUMPLIMIENTO REGULATORIO Y CONTROL INTERNO": "AGE_013",
    "INFORME CUMPLIMIENTO REGULATORIO - MIGRACION": "AGE_013",
    "INFORME RECLAMOS Y O DENUNCIAS": "AGE_014",
    "TABLA ESTANDARIZADA DE COMISIONES PARA AGENTES": "AGE_015",
    "LISTADO DE COMISIONES - MIGRACION": "AGE_015",
    "CANTIDAD DE CLIENTES": "AGE_016",
    "APERTURA DE CUENTA": "AGE_017",
    "NÓMINA DE AGENTES CON CONTRATO Y REFERENCIAMIENTO DE CLIENTES": "AGE_019",
    "NÓMINA DE AGENTES CON LOS QUE TENGA CONTRATO - MIGRACION": "AGE_019",
    "RÉGIMEN INFORMATIVO DE COMITENTES QUE OPEREN CON CDI Y CIE": "AGE_025",
    "PUBLICIDAD Y/O DIFUSIÓN": "AGE_026",
    "CAPTACIÓN DE ÓRDENES Y MODALIDAD DE CONTACTO CON CLIENTES": "AGE_028",
    "MODALIDADES DE CONTACTO - MEDIOS DE CAPTACIÓN": "AGE_028",
    "MEDIOS DE CONTACTO CON CLIENTES - MIGRACION": "AGE_028",
    "CONTRAPARTIDA LÍQUIDA - ACTIVOS ELEGIBLES VIGENTES": "AGE_029",
    "CONTRAPARTIDA LÍQUIDA SEMANAL": "AGE_029",
    "PASIVOS FINANCIEROS": "AGE_030",
    "ESTADOS CONTABLES - AGENTES": "ECF_010",
    "ESTADOS CONTABLES - COMERCIALES": "ECF_002",
    "ESTADOS CONTABLES - NIIF": "ECF_003",
    "ESTADOS CONTABLES - NIIF PARA BANCOS Y ENTIDADES FINANCIERAS": "ECF_004",
    "BALANCE CONSOLIDADO - MIGRACION": "ECF_003",
    "MANUAL DE PROCEDIMIENTOS PARA LA PLA/FT ART. 8": "PLAyFT_06",
    "MANUALES DE PROCEDIMIENTO - MIGRACION": "PLAyFT_06",
    "CÓDIGO DE CONDUCTA PARA LA PLA/FT ART. 20": "PLAyFT_07",
    "CÓDIGO DE CONDUCTA - MIGRACION": "PLAyFT_07",
    "CURSADA DE LA CAPACITACIÓN ART. 18 Y ART. 26 INC. 2": "PLAyFT_08",
    "PROGRAMA ANUAL DE CAPACITACIONES INTERNAS ART. 7 INC. O Y ART. 18": "PLAyFT_09",
    "AUTOEVALUACIÓN DE RIESGO ART. 4": "PLAyFT_10",
    "DEBIDA DILIGENCIA PREVIA DE OTRO SO PLA/FT ART. 31": "PLAyFT_11",
    "EXTERNALIZACIÓN DE TAREAS PLA/FT ART. 16": "PLAyFT_12",
    "INFORME DE CONTROL INTERNO PLAYFT ART19B": "PLAyFT_13",
    "INFORME DE REVISOR EXTERNO INDEPENDIENTE DE PLA/FT": "PLAyFT_14",
    "PERFILES TRANSACCIONALES": "PLAyFT_15",
    "DECLARACIÓN DE TOLERANCIA AL RIESGO ART 6A": "PLAyFT_16",
    "PROCEDIMIENTOS DE GESTIÓN DE ALERTAS": "PLAyFT_17",
    "REGISTRO DE ALERTAS": "PLAyFT_18",
    "SISTEMAS DE MONITOREO TRANSACCIONAL ANÁLISIS ART. 36": "PLAyFT_19",
    "OFICIALES DE CUMPLIMIENTO ARTICULO 11": "PLAyFT_05",
}

CODIGOS_VALIDOS = set(NOMBRE_A_CODIGO.values())


def fin_trimestre_anterior():
    m = HOY.month
    if m <= 3:  return date(HOY.year - 1, 12, 31)
    if m <= 6:  return date(HOY.year, 3, 31)
    if m <= 9:  return date(HOY.year, 6, 30)
    return          date(HOY.year, 9, 30)

def fin_mes_anterior():
    return HOY.replace(day=1) - timedelta(days=1)

def miercoles_esta_semana():
    monday = HOY - timedelta(days=HOY.weekday())
    return monday + timedelta(days=2)

def calcular_vencimiento(fecha_base_str, plazo_dias, cierre_ejercicio=None):
    if not fecha_base_str or fecha_base_str in ("—", ""):
        return None
    fb = fecha_base_str.strip()
    if fb == "FIN_TRIMESTRE":
        base = fin_trimestre_anterior()
        return base + timedelta(days=plazo_dias) if plazo_dias else None
    if fb == "FIN_MES":
        base = fin_mes_anterior()
        return base + timedelta(days=plazo_dias) if plazo_dias else None
    if fb == "FIN_SEMANA":
        return miercoles_esta_semana()
    if fb == "10/01":
        return date(HOY.year, 1, 10)
    if fb == "30/04":
        return date(HOY.year, 4, 30)
    if fb == "28/08":
        return date(HOY.year, 8, 28)
    if fb == "31/12":
        base = date(HOY.year - 1, 12, 31)
        return base + timedelta(days=plazo_dias) if plazo_dias else None
    if fb == "CIERRE_EJERCICIO":
        if cierre_ejercicio and plazo_dias:
            return cierre_ejercicio + timedelta(days=plazo_dias)
        return None
    return None

def calcular_estado(fecha_pres, fecha_base_str, plazo_dias, cierre_ejercicio=None):
    vencimiento = calcular_vencimiento(fecha_base_str, plazo_dias, cierre_ejercicio)
    if vencimiento is None and not plazo_dias:
        return "CUMPLIDO" if fecha_pres else "AUSENTE"
    if vencimiento:
        dias = (vencimiento - HOY).days
        if dias < 0:           return "VENCIDO"
        if dias <= PROX_DIAS:  return "PRÓXIMO"
        return "CUMPLIDO"
    if fecha_pres is None:
        return "AUSENTE"
    if plazo_dias:
        dias = (fecha_pres + timedelta(days=plazo_dias) - HOY).days
        if dias < 0:           return "VENCIDO"
        if dias <= PROX_DIAS:  return "PRÓXIMO"
    return "CUMPLIDO"

def es_agrupador(fila):
    if not any(fila):
        return True
    col_a = fila[0].strip() if len(fila) > 0 else ""
    col_b = fila[1].strip() if len(fila) > 1 else ""
    if "▶" in col_a or "▶" in col_b:
        return True
    if col_b and not (col_b.startswith("MUG_") or col_b.startswith("AGE_") or
                      col_b.startswith("ECF_") or col_b.startswith("PLAyFT_")):
        return True
    return False


def conectar_sheet():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc    = gspread.authorize(creds)
    return gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])

def leer_clientes(sheet):
    ws       = sheet.worksheet("CONFIGURACIÓN")
    all_rows = ws.get_all_values()
    if len(all_rows) < 7:
        return []
    encabezados = all_rows[5]
    clientes = []
    for fila in all_rows[6:]:
        if not any(fila):
            continue
        registro = dict(zip(encabezados, fila))
        if str(registro.get("ACTIVO (S/N)", "")).upper() == "S":
            clientes.append(registro)
    return clientes

def obtener_cierre_ejercicio(cliente_registro):
    cierre_str = cliente_registro.get("FECHA CIERRE EJERCICIO", "").strip()
    if not cierre_str:
        return None
    for fmt in ("%d/%m/%Y", "%d/%m"):
        try:
            d = datetime.strptime(cierre_str, fmt)
            if fmt == "%d/%m":
                anio = HOY.year - 1 if d.month > HOY.month else HOY.year
                d = d.replace(year=anio)
            return d.date()
        except ValueError:
            continue
    return None


def color_rgb(r, g, b):
    return {"red": r/255, "green": g/255, "blue": b/255}

def aplicar_formato_pestana(sheet, ws):
    sheet_id = ws.id
    sid      = sheet.id
    requests = [
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                      "startColumnIndex": 0, "endColumnIndex": 14},
            "cell": {"userEnteredFormat": {
                "backgroundColor": color_rgb(31,56,100),
                "textFormat": {"foregroundColor": color_rgb(255,255,255),
                               "bold": True, "fontSize": 13, "fontFamily": "Arial"},
                "verticalAlignment": "MIDDLE",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment)"
        }},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4,
                      "startColumnIndex": 0, "endColumnIndex": 14},
            "cell": {"userEnteredFormat": {
                "backgroundColor": color_rgb(242,242,242),
                "textFormat": {"italic": True, "fontSize": 9, "fontFamily": "Arial",
                               "foregroundColor": color_rgb(85,85,85)},
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat)"
        }},
        # Leyenda estados fila 6
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 5, "endRowIndex": 6,
                      "startColumnIndex": 2, "endColumnIndex": 3},
            "cell": {"userEnteredFormat": {
                "backgroundColor": color_rgb(198,239,206),
                "textFormat": {"foregroundColor": color_rgb(39,98,33), "bold": True,
                               "fontSize": 9, "fontFamily": "Arial"},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
        }},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 5, "endRowIndex": 6,
                      "startColumnIndex": 3, "endColumnIndex": 4},
            "cell": {"userEnteredFormat": {
                "backgroundColor": color_rgb(255,235,156),
                "textFormat": {"foregroundColor": color_rgb(156,87,0), "bold": True,
                               "fontSize": 9, "fontFamily": "Arial"},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
        }},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 5, "endRowIndex": 6,
                      "startColumnIndex": 4, "endColumnIndex": 6},
            "cell": {"userEnteredFormat": {
                "backgroundColor": color_rgb(255,199,206),
                "textFormat": {"foregroundColor": color_rgb(156,0,6), "bold": True,
                               "fontSize": 9, "fontFamily": "Arial"},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
        }},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 7, "endRowIndex": 8,
                      "startColumnIndex": 0, "endColumnIndex": 14},
            "cell": {"userEnteredFormat": {
                "backgroundColor": color_rgb(46,117,182),
                "textFormat": {"foregroundColor": color_rgb(255,255,255),
                               "bold": True, "fontSize": 9, "fontFamily": "Arial"},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "wrapStrategy": "WRAP",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)"
        }},
        # Formato condicional col L
        {"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": sheet_id, "startRowIndex": 8,
                        "startColumnIndex": 11, "endColumnIndex": 12}],
            "booleanRule": {
                "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "CUMPLIDO"}]},
                "format": {"backgroundColor": color_rgb(198,239,206),
                           "textFormat": {"foregroundColor": color_rgb(39,98,33), "bold": True}}
            }
        }, "index": 0}},
        {"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": sheet_id, "startRowIndex": 8,
                        "startColumnIndex": 11, "endColumnIndex": 12}],
            "booleanRule": {
                "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "PRÓXIMO"}]},
                "format": {"backgroundColor": color_rgb(255,235,156),
                           "textFormat": {"foregroundColor": color_rgb(156,87,0), "bold": True}}
            }
        }, "index": 1}},
        {"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": sheet_id, "startRowIndex": 8,
                        "startColumnIndex": 11, "endColumnIndex": 12}],
            "booleanRule": {
                "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "VENCIDO"}]},
                "format": {"backgroundColor": color_rgb(255,199,206),
                           "textFormat": {"foregroundColor": color_rgb(156,0,6), "bold": True}}
            }
        }, "index": 2}},
        {"addConditionalFormatRule": {"rule": {
            "ranges": [{"sheetId": sheet_id, "startRowIndex": 8,
                        "startColumnIndex": 11, "endColumnIndex": 12}],
            "booleanRule": {
                "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "AUSENTE"}]},
                "format": {"backgroundColor": color_rgb(255,199,206),
                           "textFormat": {"foregroundColor": color_rgb(156,0,6), "bold": True}}
            }
        }, "index": 3}},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimensi
