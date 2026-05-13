import os, json, time, re, requests
from datetime import datetime, date, timedelta
import holidays
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

# Feriados nacionales de Argentina (incluye traslados y feriados con fines turísticos).
# Cubrimos ±1 año para que sumar_habiles funcione cerca de cambios de año.
AR_HOLIDAYS = holidays.Argentina(years=range(HOY.year - 1, HOY.year + 2))

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


# ─────────────────────────────────────────────────────────────────
# Helpers de fechas y plazos
# ─────────────────────────────────────────────────────────────────

def sumar_habiles(base, n):
    """Suma n días hábiles a base_date (excluye sábados, domingos y feriados AR)."""
    d = base
    while n > 0:
        d += timedelta(days=1)
        if d.weekday() < 5 and d not in AR_HOLIDAYS:
            n -= 1
    return d


def parse_plazo_desc(desc):
    """
    Parsea descripciones tipo '10 días hábiles del cierre' o '70 días corridos del cierre'.
    Devuelve (n_dias, tipo) donde tipo es 'habiles' o 'corridos'.
    Si la descripción no especifica nro de días + tipo, devuelve (None, None)
    y el caller debe caer al valor numérico de la columna G.
    """
    if not desc:
        return None, None
    m = re.search(r'(\d+)\s*d[ií]as?\s*(h[áa]biles?|corridos?)', desc.lower())
    if not m:
        return None, None
    n = int(m.group(1))
    tipo = "habiles" if m.group(2).startswith(("hábil", "habil")) else "corridos"
    return n, tipo


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


def calcular_vencimiento(fecha_base_str, plazo_dias, plazo_desc=None, cierre_ejercicio=None):
    """
    Calcula la fecha de vencimiento.
    - Si plazo_desc contiene 'hábiles', usa sumar_habiles (excluye fines de semana y feriados).
    - Si plazo_desc contiene 'corridos' o no especifica tipo, suma días calendario.
    - Si plazo_desc trae un número explícito (ej. '10 días hábiles'), prevalece sobre la columna G.
      Si no, se usa el valor numérico de la columna G del Sheet.
    """
    if not fecha_base_str or fecha_base_str in ("—", ""):
        return None
    fb = fecha_base_str.strip()

    n_parsed, tipo = parse_plazo_desc(plazo_desc)
    n = n_parsed if n_parsed is not None else plazo_dias
    es_habiles = (tipo == "habiles")

    def sumar(base):
        if n is None:
            return None
        return sumar_habiles(base, n) if es_habiles else base + timedelta(days=n)

    if fb == "FIN_TRIMESTRE":     return sumar(fin_trimestre_anterior())
    if fb == "FIN_MES":           return sumar(fin_mes_anterior())
    if fb == "FIN_SEMANA":        return miercoles_esta_semana()
    if fb == "10/01":             return date(HOY.year, 1, 10)
    if fb == "30/04":             return date(HOY.year, 4, 30)
    if fb == "28/08":             return date(HOY.year, 8, 28)
    if fb == "31/12":             return sumar(date(HOY.year - 1, 12, 31))
    if fb == "CIERRE_EJERCICIO":
        return sumar(cierre_ejercicio) if cierre_ejercicio else None
    return None


def calcular_estado(fecha_pres, fecha_base_str, plazo_dias, plazo_desc=None, cierre_ejercicio=None):
    """
    Devuelve uno de: CUMPLIDO, PRÓXIMO, VENCIDO, AUSENTE.

    Lógica:
    - CUMPLIDO: hay presentación para el período vigente Y fue en o antes del vencimiento.
    - VENCIDO: (a) no hay presentación del período vigente y el vencimiento ya pasó,
               o (b) hay presentación pero fue posterior al vencimiento (fuera de término).
    - PRÓXIMO: sin presentación del período vigente y vencimiento dentro de los PROX_DIAS.
    - AUSENTE: sin presentación, vencimiento lejano (o sin plazo definido).
    """
    vencimiento = calcular_vencimiento(fecha_base_str, plazo_dias, plazo_desc, cierre_ejercicio)

    # Caso eventual sin plazo (ante cambio, ante apertura, etc.)
    if vencimiento is None and not plazo_dias:
        return "CUMPLIDO" if fecha_pres else "AUSENTE"

    if vencimiento:
        fb = fecha_base_str.strip() if fecha_base_str else ""

        # Inicio del período vigente: la presentación tiene que ser POSTERIOR a esta fecha
        # para contarse como del período actual (no de uno anterior).
        if fb == "FIN_TRIMESTRE":
            periodo_inicio = fin_trimestre_anterior()
        elif fb == "FIN_MES":
            periodo_inicio = fin_mes_anterior()
        elif fb == "FIN_SEMANA":
            monday = HOY - timedelta(days=HOY.weekday())
            periodo_inicio = monday - timedelta(days=7)
        elif fb in ("10/01", "30/04", "28/08"):
            periodo_inicio = date(HOY.year - 1, 12, 31)
        elif fb == "31/12":
            periodo_inicio = date(HOY.year - 2, 12, 31)
        elif fb == "CIERRE_EJERCICIO":
            # Fix: el período vigente arranca EN el cierre, no 365 días antes.
            periodo_inicio = cierre_ejercicio
        else:
            periodo_inicio = None

        # ¿La presentación corresponde al período vigente?
        if fecha_pres and (periodo_inicio is None or fecha_pres > periodo_inicio):
            # Fix principal: comparar fecha_pres contra el vencimiento, no contra HOY.
            # Si presentó antes o el mismo día del vencimiento → CUMPLIDO,
            # aunque HOY ya esté más allá del vencimiento.
            if fecha_pres <= vencimiento:
                return "CUMPLIDO"
            else:
                return "VENCIDO"   # presentó fuera de término

        # Sin presentación para el período vigente
        dias = (vencimiento - HOY).days
        if dias < 0:           return "VENCIDO"
        if dias <= PROX_DIAS:  return "PRÓXIMO"
        return "AUSENTE"

    # Caso: sin vencimiento absoluto pero con plazo desde fecha_pres (eventual con plazo)
    if fecha_pres is None: return "AUSENTE"
    if plazo_dias:
        n_parsed, tipo = parse_plazo_desc(plazo_desc)
        n = n_parsed if n_parsed is not None else plazo_dias
        if tipo == "habiles":
            limite = sumar_habiles(fecha_pres, n)
        else:
            limite = fecha_pres + timedelta(days=n)
        dias = (limite - HOY).days
        if dias < 0:           return "VENCIDO"
        if dias <= PROX_DIAS:  return "PRÓXIMO"
    return "CUMPLIDO"


def debe_correr_hoy(frecuencia_str):
    f = frecuencia_str.strip().upper() if frecuencia_str else "DIARIA"
    if f in ("DIARIA", ""):            return True
    if f == "LUNES":                   return HOY.weekday() == 0
    if f == "MARTES":                  return HOY.weekday() == 1
    if f in ("MIÉRCOLES","MIERCOLES"): return HOY.weekday() == 2
    if f == "JUEVES":                  return HOY.weekday() == 3
    if f == "VIERNES":                 return HOY.weekday() == 4
    if f == "SEMANAL":                 return HOY.weekday() == 0
    if f in ("MENSUAL","PRIMER DIA MES","PRIMERO MES"): return HOY.day == 1
    if f == "DIA 15":                  return HOY.day == 15
    if f in ("ULTIMO DIA MES","ÚLTIMO DIA MES"):
        return (HOY + timedelta(days=1)).month != HOY.month
    return True


def es_agrupador(fila):
    if not any(fila): return True
    col_a = fila[0].strip() if len(fila) > 0 else ""
    col_b = fila[1].strip() if len(fila) > 1 else ""
    if "▶" in col_a or "▶" in col_b: return True
    if not col_b: return True
    if not (col_b.startswith("MUG_") or col_b.startswith("AGE_") or
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
    print(f"[INFO] Encabezados: {encabezados}")
    clientes = []
    for i, fila in enumerate(all_rows[6:], start=7):
        if not any(fila): continue
        registro = dict(zip(encabezados, fila))
        ejecutar = str(
            registro.get("EJECUTAR EN PRÓX. CRON", "") or
            registro.get("EJECUTAR EN PROX. CRON", "")
        ).strip().upper()
        usuario = registro.get("USUARIO AIF", "").strip()
        print(f"[INFO] Fila {i}: nombre='{registro.get('NOMBRE CLIENTE')}' "
              f"ejecutar='{ejecutar}' usuario='{usuario}'")
        if ejecutar == "S" and usuario:
            registro["_row"] = i
            clientes.append(registro)
    print(f"[INFO] Total clientes a procesar: {len(clientes)}")
    return clientes

def obtener_cierre_ejercicio(cliente_registro):
    cierre_str = cliente_registro.get("FECHA CIERRE EJERCICIO", "").strip()
    if not cierre_str: return None
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


def obtener_o_crear_pestana(sheet, nombre_pestana, tipo, nombre_cliente):
    """
    Si la pestaña ya existe: la reutiliza limpiando columnas I:L.
    Si no existe: duplica el template del tipo correspondiente,
    preservando todo el formato, anchos de columna, colores, etc.
    """
    try:
        ws = sheet.worksheet(nombre_pestana)
        print(f"  [PESTAÑA] Reutilizando '{nombre_pestana}'")
        # Limpiar estados anteriores para reprocesar
        all_vals = ws.get_all_values()
        batch = []
        for i, fila in enumerate(all_vals[8:], start=9):
            if not es_agrupador(fila):
                batch.append({"range": f"I{i}:L{i}",
                              "values": [["", "", "", "PENDIENTE"]]})
        if batch:
            ws.batch_update(batch)
            time.sleep(2)
        return ws

    except gspread.WorksheetNotFound:
        # Duplicar la pestaña template — preserva TODO el formato
        nombre_template = f"{tipo} - OBLIGACIONES"
        print(f"  [PESTAÑA] Creando '{nombre_pestana}' desde template '{nombre_template}'")
        try:
            ws_template = sheet.worksheet(nombre_template)
        except gspread.WorksheetNotFound:
            raise Exception(
                f"No existe la pestaña template '{nombre_template}'. "
                f"Verificá que exista en el Sheet."
            )

        sid               = sheet.id
        template_sheet_id = ws_template.id

        sheet.client.request(
            "post",
            f"https://sheets.googleapis.com/v4/spreadsheets/{sid}:batchUpdate",
            json={"requests": [{"duplicateSheet": {
                "sourceSheetId":    template_sheet_id,
                "insertSheetIndex": len(sheet.worksheets()),
                "newSheetName":     nombre_pestana,
            }}]}
        )
        time.sleep(2)

        ws = sheet.worksheet(nombre_pestana)

        # Escribir nombre del cliente en la celda del título (fila 2, col B)
        ws.update_cell(2, 2,
            f"{nombre_cliente}  ({tipo})  |  Régimen Informativo AIF — CNV")
        time.sleep(1)

        return ws


def escribir_log(sheet, cliente, codigo, descripcion, estado_ant, estado_nuevo, fecha_pres):
    ws = sheet.worksheet("LOG")
    ws.append_row([
        AHORA_AR.strftime("%d/%m/%Y %H:%M"),
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
                    AHORA_AR.strftime("%d/%m/%Y %H:%M"),
                ]]
            )
            time.sleep(1)
            return


def scrape_cliente(page, usuario, password):
    """Scraping con reintentos ante timeout de CNV."""
    MAX_INTENTOS = 3

    for intento in range(MAX_INTENTOS):
        try:
            print(f"  [LOGIN] Intento {intento + 1}/{MAX_INTENTOS}...")
            adfs_url = (
                "https://cnvfs.cnv.gov.ar/adfs/ls/"
                "?wtrealm=https://aif2.cnv.gov.ar"
                "&wa=wsignin1.0"
                "&wreply=https://aif2.cnv.gov.ar/"
            )
            page.goto(adfs_url, timeout=90000)
            page.wait_for_load_state("networkidle", timeout=90000)
            page.wait_for_selector("input[name='UserName']", timeout=60000)
            page.fill("input[name='UserName']", usuario)
            page.fill("input[name='Password']", password)
            page.click("#submitButton")
            page.wait_for_load_state("networkidle", timeout=90000)

            print(f"  [LOGIN] URL post-login: {page.url}")

            page.goto("https://aif2.cnv.gov.ar/Administered/History",
                      timeout=90000)
            page.wait_for_load_state("networkidle", timeout=90000)
            page.wait_for_selector("#grid-presentations tbody tr",
                                   timeout=30000)
            break

        except Exception as e:
            print(f"  [LOGIN] Error intento {intento + 1}: {e}")
            if intento < MAX_INTENTOS - 1:
                print(f"  [LOGIN] Esperando 30s antes de reintentar...")
                time.sleep(30)
                try:
                    page.goto("about:blank")
                except Exception:
                    pass
            else:
                raise

    # Extraer presentaciones
    presentaciones = []
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
            if len(celdas) < 7: continue
            pres_id   = celdas[0].inner_text().strip()
            fecha_str = celdas[1].inner_text().strip()
            hora_str  = celdas[2].inner_text().strip()
            span = celdas[3].query_selector("span[style*='font-weight']")
            nombre_form = (span.inner_text().strip() if span
                           else celdas[3].inner_text().strip())
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
        siguiente = page.query_selector(
            "li.next:not(.disabled) a[data-page='next']")
        if not siguiente: break
        siguiente.click()
        for _ in range(15):
            page.wait_for_timeout(1000)
            n = len(page.query_selector_all("#grid-presentations tbody tr"))
            if n > 1: break

    print(f"  Total presentaciones extraídas: {len(presentaciones)}")
    try:
        page.goto("https://aif2.cnv.gov.ar/Home/Logout")
    except Exception:
        pass
    return presentaciones


def main():
    print("[INFO] Iniciando relevamiento...")
    try:
        sheet = conectar_sheet()
        print("[INFO] Conexión establecida")
    except Exception as e:
        print(f"[ERROR FATAL] No se pudo conectar: {e}")
        return

    clientes = leer_clientes(sheet)

    if not clientes:
        print("[INFO] No hay clientes con EJECUTAR EN PRÓX. CRON = S. Finalizando.")
        return

    # Verificar que existan los templates necesarios antes de empezar
    tipos_necesarios = set(c.get("TIPO (AN/ALyC)", "").strip() for c in clientes)
    for tipo in tipos_necesarios:
        nombre_template = f"{tipo} - OBLIGACIONES"
        try:
            sheet.worksheet(nombre_template)
            print(f"[INFO] Template '{nombre_template}' encontrado OK")
        except gspread.WorksheetNotFound:
            print(f"[WARN] Template '{nombre_template}' NO encontrado — "
                  f"los clientes de tipo {tipo} van a fallar")
    time.sleep(1)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)

        for cliente in clientes:
            nombre           = cliente.get("NOMBRE CLIENTE", "").strip()
            tipo             = cliente.get("TIPO (AN/ALyC)", "").strip()
            usuario          = cliente.get("USUARIO AIF", "").strip()
            password         = cliente.get("CLAVE AIF", "").strip()
            frecuencia       = cliente.get("FRECUENCIA RELEV.", "DIARIA")
            mail_contacto    = cliente.get("MAIL CONTACTO", "").strip()
            cierre_ejercicio = obtener_cierre_ejercicio(cliente)

            if not password:
                print(f"[SKIP] {nombre}: sin clave AIF")
                continue

            if not debe_correr_hoy(frecuencia):
                print(f"[SKIP] {nombre}: frecuencia '{frecuencia}' no corresponde hoy")
                continue

            if tipo not in ("ALyC", "AN", "AAGI"):
                print(f"[SKIP] {nombre}: tipo desconocido '{tipo}'")
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

            fecha_corta    = AHORA_AR.strftime("%d/%m")
            nombre_pestana = f"{nombre} · {tipo} · {fecha_corta}"

            try:
                ws_cliente = obtener_o_crear_pestana(
                    sheet, nombre_pestana, tipo, nombre)
            except Exception as e:
                print(f"[ERROR] {nombre}: no se pudo crear pestaña — {e}")
                continue
            time.sleep(2)

            # ── Marcar PROCESANDO en A1 ────────────────────────────────
            # El Apps Script ignora esta pestaña hasta que llegue "LISTO"
            try:
                ws_cliente.update_cell(1, 1, "PROCESANDO")
                time.sleep(1)
            except Exception:
                pass

            try:
                ws_cliente.update_cell(4, 2,
                    f"Relevamiento: {AHORA_AR.strftime('%d/%m/%Y %H:%M')} (hora Argentina)")
                time.sleep(1)
            except Exception:
                pass

            obligaciones = ws_cliente.get_all_values()
            time.sleep(1)

            conteo = {"total": 0, "cumplidas": 0, "proximas": 0, "vencidas": 0}
            actualizaciones = []

            for i, fila in enumerate(obligaciones[8:]):
                if es_agrupador(fila): continue
                codigo = fila[1].strip() if len(fila) > 1 else ""
                if not codigo: continue

                descripcion = fila[2].strip()  if len(fila) > 2  else ""
                plazo_desc  = fila[5].strip()  if len(fila) > 5  else ""   # nuevo: columna F
                plazo_str   = fila[6].strip()  if len(fila) > 6  else ""
                plazo_dias  = int(plazo_str)   if plazo_str.isdigit() else None
                fecha_base  = fila[7].strip()  if len(fila) > 7  else ""
                estado_ant  = fila[11].strip() if len(fila) > 11 else ""

                if estado_ant == "N/A": continue

                match      = next((p for p in presentaciones
                                   if NOMBRE_A_CODIGO.get(p["nombre"]) == codigo), None)
                fecha_pres = match["fecha"] if match else None
                hora_pres  = match["hora"]  if match else ""
                id_pres    = match["id"]    if match else ""

                estado_nuevo = calcular_estado(
                    fecha_pres, fecha_base, plazo_dias, plazo_desc, cierre_ejercicio)

                conteo["total"] += 1
                if estado_nuevo == "CUMPLIDO":   conteo["cumplidas"] += 1
                elif estado_nuevo == "PRÓXIMO":  conteo["proximas"]  += 1
                else:                            conteo["vencidas"]  += 1

                if estado_nuevo != estado_ant or (match and not fila[8].strip()):
                    actualizaciones.append({
                        "row":         i + 9,
                        "fecha":       fecha_pres,
                        "hora":        hora_pres,
                        "id":          id_pres,
                        "estado":      estado_nuevo,
                        "codigo":      codigo,
                        "descripcion": descripcion,
                        "estado_ant":  estado_ant,
                    })

            for upd in actualizaciones:
                ws_cliente.update_cell(upd["row"], 9,
                    upd["fecha"].strftime("%d/%m/%Y") if upd["fecha"] else "")
                time.sleep(1)
                ws_cliente.update_cell(upd["row"], 10, upd["hora"])
                time.sleep(1)
                ws_cliente.update_cell(upd["row"], 11, upd["id"])
                time.sleep(1)
                ws_cliente.update_cell(upd["row"], 12, upd["estado"])
                time.sleep(1)
                if upd["estado"] != upd["estado_ant"]:
                    escribir_log(sheet, nombre, upd["codigo"],
                                 upd["descripcion"], upd["estado_ant"],
                                 upd["estado"], upd["fecha"])
                    print(f"  [{upd['codigo']}] {upd['estado_ant']} → {upd['estado']}")

            actualizar_dashboard(sheet, nombre, conteo["total"],
                                 conteo["cumplidas"], conteo["proximas"],
                                 conteo["vencidas"])

            # ── Señal final: A1 = "LISTO" ──────────────────────────────
            # En este punto todos los estados están escritos en el Sheet.
            # El Apps Script (trigger cada 5 min) detecta "LISTO" y exporta.
            if mail_contacto:
                try:
                    ws_cliente.update_cell(1, 1, "LISTO")
                    time.sleep(1)
                    print(f"  [PDF] Centinela 'LISTO' escrito en A1 — Apps Script generará el PDF")
                except Exception as e:
                    print(f"  [WARN] No se pudo escribir centinela PDF: {e}")
            else:
                try:
                    ws_cliente.update_cell(1, 1, "SIN_MAIL")
                except Exception:
                    pass
                print(f"  [INFO] {nombre}: sin mail, no se genera PDF")

            print(f"[DONE] {nombre}: {conteo}")
            print(f"[INFO] Esperando 60s antes del próximo cliente...")
            time.sleep(60)

        browser.close()

    print("[INFO] Relevamiento finalizado.")


if __name__ == "__main__":
    main()
