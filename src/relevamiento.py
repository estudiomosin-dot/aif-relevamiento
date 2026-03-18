import os, json
from datetime import datetime, date, timedelta
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
HOY       = date.today()
PROX_DIAS = 30

def calcular_estado(fecha_pres, plazo_dias):
    if fecha_pres is None:
        return "AUSENTE"
    if plazo_dias is None:
        return "CUMPLIDO"
    vencimiento    = fecha_pres + timedelta(days=plazo_dias)
    dias_restantes = (vencimiento - HOY).days
    if dias_restantes < 0:
        return "VENCIDO"
    if dias_restantes <= PROX_DIAS:
        return "PRÓXIMO"
    return "CUMPLIDO"

def conectar_sheet():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc    = gspread.authorize(creds)
    return gc.open_by_key(os.environ["GOOGLE_SHEET_ID"])

def leer_clientes(sheet):
    ws   = sheet.worksheet("CONFIGURACIÓN")
    rows = ws.get_all_records()
    return [r for r in rows if str(r.get("ACTIVO (S/N)", "")).upper() == "S"]

def actualizar_formulario(sheet, tipo, codigo, fecha_pres, estado):
    nombre_hoja = "ALyC - OBLIGACIONES" if tipo == "ALyC" else "AN - OBLIGACIONES"
    ws    = sheet.worksheet(nombre_hoja)
    datos = ws.get_all_values()
    for i, fila in enumerate(datos):
        if fila and fila[1].strip() == codigo:
            row_num = i + 1
            ws.update_cell(row_num, 7, fecha_pres.strftime("%d/%m/%Y") if fecha_pres else "")
            ws.update_cell(row_num, 8, estado)
            return True
    return False

def escribir_log(sheet, cliente, codigo, estado_ant, estado_nuevo, fecha_pres):
    ws = sheet.worksheet("LOG")
    ws.append_row([
        datetime.now().strftime("%d/%m/%Y %H:%M"),
        cliente, codigo, estado_ant, estado_nuevo,
        fecha_pres.strftime("%d/%m/%Y") if fecha_pres else "",
        "",
    ])

def actualizar_dashboard(sheet, cliente, total, cumplidas, proximas, vencidas):
    ws    = sheet.worksheet("DASHBOARD")
    datos = ws.get_all_values()
    for i, fila in enumerate(datos):
        if fila and fila[1].strip() == cliente:
            row_num = i + 1
            ws.update(f"E{row_num}:I{row_num}", [[
                cumplidas, proximas, vencidas,
                f"=E{row_num}/D{row_num}",
                datetime.now().strftime("%d/%m/%Y %H:%M"),
            ]])
            return

def scrape_cliente(page, usuario, password):
    """
    TODO: completar con los selectores reales de la AIF.
    Retorna lista de dicts: [{"codigo": "MUG_001", "fecha": date(2025,3,1)}, ...]
    """
    presentaciones = []
    # page.goto("https://aif.cnv.gov.ar/")
    # page.fill("#usuario", usuario)
    # page.fill("#password", password)
    # page.click("#btn-ingresar")
    # page.wait_for_load_state
