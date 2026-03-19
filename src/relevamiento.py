import os, json, time
from datetime import datetime, date, timedelta
from calendar import monthrange
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
HOY       = date.today()
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
    # PLAyFT
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

# ── Lógica de vencimiento ────────────────────────────────────────────────
def fin_trimestre_anterior():
    """Último día del trimestre anterior al día de hoy."""
    m = HOY.month
    if m <= 3:   return date(HOY.year - 1, 12, 31)
    if m <= 6:   return date(HOY.year, 3, 31)
    if m <= 9:   return date(HOY.year, 6, 30)
    return           date(HOY.year, 9, 30)

def fin_mes_anterior():
    """Último día del mes anterior."""
    first = HOY.replace(day=1)
    return first - timedelta(days=1)

def fin_semana_anterior():
    """Último día (domingo) de la semana anterior."""
    days_since_monday = HOY.weekday()
    last_monday = HOY - timedelta(days=days_since_monday)
    return last_monday - timedelta(days=1)  # domingo anterior

def miercoles_esta_semana():
    """Miércoles de la semana actual (vencimiento AGE_025 y AGE_029)."""
    days_since_monday = HOY.weekday()
    monday = HOY - timedelta(days=days_since_monday)
    return monday + timedelta(days=2)  # miércoles

def calcular_vencimiento(fecha_base_str, plazo_dias, cierre_ejercicio=None):
    """
    Calcula la fecha de vencimiento según la fecha base y plazo.
    Retorna date o None si no aplica.
    """
    if not fecha_base_str or fecha_base_str in ("—", ""):
        return None  # eventual puro — sin vencimiento fijo

    fb = fecha_base_str.strip()

    if fb == "FIN_TRIMESTRE":
        base = fin_trimestre_anterior()
    elif fb == "FIN_MES":
        base = fin_mes_anterior()
    elif fb == "FIN_SEMANA":
        # Para semanales el vencimiento es el miércoles de la semana en curso
        return miercoles_esta_semana()
    elif fb == "10/01":
        # Vence el 10 de enero de cada año
        return date(HOY.year, 1, 10)
    elif fb == "30/04":
        return date(HOY.year, 4, 30)
    elif fb == "28/08":
        return date(HOY.year, 8, 28)
    elif fb == "31/12":
        base = date(HOY.year - 1, 12, 31)
    elif fb == "CIERRE_EJERCICIO":
        if cierre_ejercicio:
            base = cierre_ejercicio
        else:
            return None
    else:
        return None

    if plazo_dias:
        return base + timedelta(days=plazo_dias)
    return None


def calcular_estado(fecha_pres, fecha_base_str, plazo_dias, cierre_ejercicio=None):
    """
    Determina el estado del formulario.
    Para eventuales sin fecha_base: CUMPLIDO si hay presentación, AUSENTE si no.
    Para periódicos: compara HOY con el vencimiento calculado.
    """
    vencimiento = calcular_vencimiento(fecha_base_str, plazo_dias, cierre_ejercicio)

    # Eventual puro (sin fecha base ni plazo)
    if vencimiento is None and (not plazo_dias):
        if fecha_pres is None:
            return "AUSENTE"
        return "CUMPLIDO"

    # Periódico con fecha de vencimiento calculable
    if vencimiento:
        dias_restantes = (vencimiento - HOY).days
        if dias_restantes < 0:
            return "VENCIDO"
        if dias_restantes <= PROX_DIAS:
            return "PRÓXIMO"
        return "CUMPLIDO"

    # Fallback: si hay presentación reciente
    if fecha_pres is None:
        return "AUSENTE"
    if plazo_dias:
        venc_desde_pres = fecha_pres + timedelta(days=plazo_dias)
        dias = (venc_desde_pres - HOY).days
        if dias < 0:   return "VENCIDO"
        if dias <= PROX_DIAS: return "PRÓXIMO"
        return "CUMPLIDO"
    return "CUMPLIDO"


# ── Google Sheets ────────────────────────────────────────────────────────
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
    """Lee la fecha de cierre de ejercicio de la fila del cliente en CONFIGURACIÓN."""
    cierre_str = cliente_registro.get("FECHA CIERRE EJERCICIO", "").strip()
    if not cierre_str:
        return None
    for fmt in ("%d/%m/%Y", "%d/%m", "%Y-%m-%d"):
        try:
            d = datetime.strptime(cierre_str, fmt)
            # Si solo tiene día/mes, usar el año anterior más cercano
            if fmt == "%d/%m":
                d = d.replace(year=HOY.year - 1 if d.month > HOY.month else HOY.year)
            return d.date()
        except ValueError:
            continue
    return None


def obtener_o_crear_pestana(sheet, nombre_pestana, plantilla_datos):
    try:
        ws = sheet.worksheet(nombre_pestana)
        # Limpiar cols I, J, K, L desde fila 9
        all_vals = ws.get_all_values()
        for i, fila in enumerate(all_vals[8:], start=9):
            if fila and len(fila) > 1 and fila[1] and not fila[1].startswith("▶"):
                ws.update_cell(i, 9,  "")
                ws.update_cell(i, 10, "")
                ws.update_cell(i, 11, "")
                ws.update_cell(i, 12, "PENDIENTE")
                time.sleep(0.3)
        return ws
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=nombre_pestana, rows=250, cols=14)
        time.sleep(1)
        if plantilla_datos:
            ws.update(range_name="A1", values=plantilla_datos)
            time.sleep(2)
        return ws


def escribir_log(sheet, cliente, codigo, descripcion, estado_ant, estado_nuevo, fecha_pres):
    ws = sheet.worksheet("LOG")
    ws.append_row([
        datetime.now().strftime("%d/%m/%Y %H:%M"),
        cliente,
        f"{codigo} — {descripcion}",
        estado_ant,
        estado_nuevo,
        fecha_pres.strftime("%d/%m/%Y") if fecha_pres else "",
        "",
    ])
    time.sleep(1)


def actualizar_dashboard(sheet, cliente, total, cumplidas, proximas, vencidas):
    ws    = sheet.worksheet("DASHBOARD")
    datos = ws.get_all_values()
    for i, fila in enumerate(datos):
        if fila and fila[1].strip() == cliente:
            row_num = i + 1
            ws.update(
                range_name=f"E{row_num}:I{row_num}",
                values=[[
                    cumplidas, proximas, vencidas,
                    f"=E{row_num}/D{row_num}",
                    datetime.now().strftime("%d/%m/%Y %H:%M"),
                ]]
            )
            time.sleep(1)
            return


# ── Scraping AIF ─────────────────────────────────────────────────────────
def scrape_cliente(page, usuario, password):
    presentaciones = []

    adfs_url = (
        "https://cnvfs.cnv.gov.ar/adfs/ls/"
        "?wtrealm=https://aif2.cnv.gov.ar"
        "&wa=wsignin1.0"
        "&wreply=https://aif2.cnv.gov.ar/"
    )
    page.goto(adfs_url)
    page.wait_for_load_state("networkidle")
    page.wait_for_selector("input[name='UserName']", timeout=15000)
    page.fill("input[name='UserName']", usuario)
    page.fill("input[name='Password']", password)
    page.click("#submitButton")
    page.wait_for_load_state("networkidle")

    page.goto("https://aif2.cnv.gov.ar/Administered/History")
    page.wait_for_load_state("networkidle")
    page.wait_for_selector("#grid-presentations tbody tr", timeout=20000)
    filas_iniciales = len(page.query_selector_all("#grid-presentations tbody tr"))

    page.select_option("#date", "all")

    for intento in range(60):
        page.wait_for_timeout(1000)
        n = len(page.query_selector_all("#grid-presentations tbody tr"))
        if n > 1 and n != filas_iniciales:
            print(f"  Tabla recargada en intento {intento+1}: {n} filas")
            break
        if intento % 5 == 0:
            print(f"  Esperando recarga... intento {intento+1}, filas: {n}")
    else:
        print("  ADVERTENCIA: continuando con datos disponibles")

    while True:
        filas = page.query_selector_all("#grid-presentations tbody tr")
        print(f"  Filas en página actual: {len(filas)}")

        for fila in filas:
            celdas = fila.query_selector_all("td")
            if len(celdas) < 7:
                continue
            # col 0: ID presentación
            pres_id   = celdas[0].inner_text().strip()
            fecha_str = celdas[1].inner_text().strip()
            hora_str  = celdas[2].inner_text().strip()
            span = celdas[3].query_selector("span[style*='font-weight']")
            nombre_form = span.inner_text().strip() if span else celdas[3].inner_text().strip()
            try:
                fecha = datetime.strptime(fecha_str, "%d-%m-%Y").date()
            except ValueError:
                continue
            presentaciones.append({
                "nombre": nombre_form.upper(),
                "fecha":  fecha,
                "hora":   hora_str,
                "id":     pres_id,
            })

        siguiente = page.query_selector("li.next:not(.disabled) a[data-page='next']")
        if not siguiente:
            break
        siguiente.click()
        page.wait_for_timeout(2000)
        page.wait_for_selector("#grid-presentations tbody tr", timeout=10000)

    print(f"  Total presentaciones extraídas: {len(presentaciones)}")

    try:
        page.goto("https://aif2.cnv.gov.ar/Home/Logout")
    except Exception:
        pass

    return presentaciones


# ── Main ─────────────────────────────────────────────────────────────────
def main():
    sheet         = conectar_sheet()
    clientes      = leer_clientes(sheet)
    clientes_json = json.loads(os.environ.get("CLIENTES_JSON", "{}"))

    ws_alyc    = sheet.worksheet("ALyC - OBLIGACIONES")
    ws_an      = sheet.worksheet("AN - OBLIGACIONES")
    datos_alyc = ws_alyc.get_all_values()
    datos_an   = ws_an.get_all_values()
    time.sleep(2)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)

        for cliente in clientes:
            nombre   = cliente["NOMBRE CLIENTE"]
            tipo     = cliente["TIPO (AN/ALyC)"]
            creds    = clientes_json.get(nombre, {})
            usuario  = creds.get("usuario", "")
            password = creds.get("password", "")
            cierre_ejercicio = obtener_cierre_ejercicio(cliente)

            if not usuario or not password:
                print(f"[SKIP] {nombre}: sin credenciales")
                continue

            print(f"[START] {nombre} ({tipo})")

            ctx  = browser.new_context(locale="es-AR")
            page = ctx.new_page()

            try:
                presentaciones = scrape_cliente(page, usuario, password)
            except Exception as e:
                print(f"[ERROR] {nombre}: {e}")
                ctx.close()
                continue
            finally:
                ctx.close()

            # Obtener o crear pestaña del cliente
            nombre_pestana = f"{nombre} · {tipo}"
            plantilla      = datos_alyc if tipo == "ALyC" else datos_an
            ws_cliente     = obtener_o_crear_pestana(sheet, nombre_pestana, plantilla)
            time.sleep(2)

            # Actualizar fecha de relevamiento
            try:
                ws_cliente.update_cell(4, 2,
                    f"Relevamiento: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
                time.sleep(1)
            except Exception:
                pass

            obligaciones = ws_cliente.get_all_values()
            time.sleep(1)

            conteo = {"total": 0, "cumplidas": 0, "proximas": 0, "vencidas": 0}
            actualizaciones = []

            for i, fila in enumerate(obligaciones[8:]):
                if not fila or not fila[1]:
                    continue
                if fila[1].startswith("▶"):
                    continue
                # col L (índice 11) = estado
                if len(fila) > 11 and fila[11].strip() == "N/A":
                    continue

                codigo       = fila[1].strip()
                descripcion  = fila[2].strip() if len(fila) > 2 else ""
                # col G (índice 6) = plazo días
                plazo_str    = fila[6].strip() if len(fila) > 6 else ""
                plazo_dias   = int(plazo_str) if plazo_str.isdigit() else None
                # col H (índice 7) = fecha base
                fecha_base   = fila[7].strip() if len(fila) > 7 else ""
                # estado actual col L (índice 11)
                estado_ant   = fila[11].strip() if len(fila) > 11 else ""

                match = next(
                    (p for p in presentaciones
                     if NOMBRE_A_CODIGO.get(p["nombre"]) == codigo),
                    None
                )
                fecha_pres   = match["fecha"] if match else None
                hora_pres    = match["hora"]  if match else ""
                id_pres      = match["id"]    if match else ""

                estado_nuevo = calcular_estado(
                    fecha_pres, fecha_base, plazo_dias, cierre_ejercicio)

                conteo["total"] += 1
                if estado_nuevo == "CUMPLIDO":   conteo["cumplidas"] += 1
                elif estado_nuevo == "PRÓXIMO":  conteo["proximas"]  += 1
                else:                            conteo["vencidas"]  += 1

                if estado_nuevo != estado_ant or (match and not fila[8].strip()):
                    row_num = i + 9
                    actualizaciones.append({
                        "row":         row_num,
                        "fecha":       fecha_pres,
                        "hora":        hora_pres,
                        "id":          id_pres,
                        "estado":      estado_nuevo,
                        "codigo":      codigo,
                        "descripcion": descripcion,
                        "estado_ant":  estado_ant,
                    })

            # Escribir actualizaciones con pausa
            for upd in actualizaciones:
                # I: fecha (col 9)
                ws_cliente.update_cell(upd["row"], 9,
                    upd["fecha"].strftime("%d/%m/%Y") if upd["fecha"] else "")
                time.sleep(1)
                # J: hora (col 10)
                ws_cliente.update_cell(upd["row"], 10, upd["hora"])
                time.sleep(1)
                # K: ID (col 11)
                ws_cliente.update_cell(upd["row"], 11, upd["id"])
                time.sleep(1)
                # L: estado (col 12)
                ws_cliente.update_cell(upd["row"], 12, upd["estado"])
                time.sleep(1)

                if upd["estado"] != upd["estado_ant"]:
                    escribir_log(sheet, nombre, upd["codigo"], upd["descripcion"],
                                 upd["estado_ant"], upd["estado"], upd["fecha"])
                    print(f"  [{upd['codigo']}] {upd['estado_ant']} → {upd['estado']}")

            actualizar_dashboard(sheet, nombre, conteo["total"],
                                 conteo["cumplidas"], conteo["proximas"],
                                 conteo["vencidas"])
            print(f"[DONE] {nombre}: {conteo}")

        browser.close()


if __name__ == "__main__":
    main()
