import os, json, time
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

NOMBRE_A_CODIGO = {
    "HECHO RELEVANTE": "MUG_001",
    "DATOS BÁSICOS DEL ADMINISTRADO": "MUG_002",
    "DDJJ AIF": "MUG_003",
    "DOMICILIO ELECTRÓNICO": "MUG_004",
    "ORGANIGRAMA": "MUG_005",
    "SOLICITUD DE ALTA Y BAJA DE CATEGORÍAS": "MUG_006",
    "COMPOSICIÓN DEL CAPITAL Y TENENCIAS": "MUG_008",
    "GRUPO ECONÓMICO – CONTROLANTES, CONTROLADAS Y VINCULADAS": "MUG_009",
    "BAJAS Y LICENCIAS DE MIEMBROS DE LOS ÓRGANOS DE ADMINISTRACIÓN Y FISCALIZACIÓN": "MUG_010",
    "NÓMINA DE AUDITORES EXTERNOS": "MUG_011",
    "NÓMINA DE CONSEJO DE VIGILANCIA": "MUG_012",
    "NÓMINA DE DIRECTORES": "MUG_013",
    "NÓMINA DE FAMILIARES DE MIEMBROS DE ÓRGANOS DE ADMINISTRACIÓN Y FISCALIZACIÓN": "MUG_014",
    "NÓMINA DE GERENTES": "MUG_015",
    "NÓMINA DE SÍNDICOS O COMISIÓN FISCALIZADORA": "MUG_016",
    "NÓMINA DEL COMITÉ DE AUDITORÍA": "MUG_017",
    "ACTA DE ASAMBLEA Y/O REUNIÓN DE SOCIOS": "MUG_021",
    "ACTA DE ÓRGANO DE ADMINISTRACIÓN (DIRECTORIO)": "MUG_022",
    "ACTA DEL ÓRGANO DE FISCALIZACIÓN": "MUG_023",
    "ACTA DE COMITÉ DE AUDITORÍA": "MUG_024",
    "CONVOCATORIA A ASAMBLEA": "MUG_025",
    "ESTATUTO VIGENTE": "MUG_028",
    "FICHA – AGENTES Y SOCIEDADES": "AGE_001",
    "MEMBRESÍAS": "AGE_002",
    "DESIGNACIÓN RESPONSABLE DE CUMPLIMIENTO REGULATORIO Y CONTROL INTERNO": "AGE_003",
    "DESIGNACIÓN RESPONSABLE DE RELACIONES CON EL PÚBLICO": "AGE_004",
    "VALORIZACIÓN DE CARTERAS ADMINISTRADAS.": "AGE_007",
    "ADELANTOS TRANSITORIOS OTORGADOS- INC. B) ART. 11, CAPÍTULO VII": "AGE_008",
    "PROCEDIMIENTO SEGREGACIÓN DE ACTIVOS": "AGE_010",
    "INFORME AUDITORÍA ANUAL DE SISTEMAS": "AGE_012",
    "INFORME PERIÓDICO DE RESPONSABLE DE CUMPLIMIENTO REGULATORIO Y CONTROL INTERNO": "AGE_013",
    "INFORME RECLAMOS Y O DENUNCIAS": "AGE_014",
    "TABLA ESTANDARIZADA DE COMISIONES PARA AGENTES": "AGE_015",
    "CANTIDAD DE CLIENTES": "AGE_016",
    "APERTURA DE CUENTA": "AGE_017",
    "NÓMINA DE AGENTES CON CONTRATO Y REFERENCIAMIENTO DE CLIENTES": "AGE_019",
    "RÉGIMEN INFORMATIVO DE COMITENTES QUE OPEREN CON CDI Y CIE": "AGE_025",
    "PUBLICIDAD Y/O DIFUSIÓN": "AGE_026",
    "CAPTACIÓN DE ÓRDENES Y MODALIDAD DE CONTACTO CON CLIENTES": "AGE_028",
    "CONTRAPARTIDA LÍQUIDA - ACTIVOS ELEGIBLES VIGENTES": "AGE_029",
    "PASIVOS FINANCIEROS": "AGE_030",
    "ESTADOS CONTABLES - BANCOS Y ENTIDADES FINANCIERAS": "ECF_001",
    "ESTADOS CONTABLES - COMERCIALES": "ECF_002",
    "ESTADOS CONTABLES - NIIF": "ECF_003",
    "ESTADOS CONTABLES - NIIF PARA BANCOS Y ENTIDADES FINANCIERAS": "ECF_004",
    "ESTADOS CONTABLES - SEGUROS": "ECF_005",
    "ESTADOS CONTABLES-FIDEICOMISOS Y AGENTES": "ECF_010",
    "ESTADOS CONTABLES - AGENTES": "ECF_010",
}


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
    ws       = sheet.worksheet("CONFIGURACIÓN")
    all_rows = ws.get_all_values()
    if len(all_rows) < 7:
        return []
    encabezados = all_rows[5]
    filas_datos = all_rows[6:]
    clientes = []
    for fila in filas_datos:
        if not any(fila):
            continue
        registro = dict(zip(encabezados, fila))
        if str(registro.get("ACTIVO (S/N)", "")).upper() == "S":
            clientes.append(registro)
    return clientes


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
                    cumplidas,
                    proximas,
                    vencidas,
                    f"=E{row_num}/D{row_num}",
                    datetime.now().strftime("%d/%m/%Y %H:%M"),
                ]]
            )
            time.sleep(1)
            return


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

    # Ir al historial
    page.goto("https://aif2.cnv.gov.ar/Administered/History")
    page.wait_for_load_state("networkidle")

    # Esperar tabla inicial con filtro por defecto (6 meses)
    page.wait_for_selector("#grid-presentations tbody tr", timeout=20000)
    filas_iniciales = len(page.query_selector_all("#grid-presentations tbody tr"))
    print(f"  Filas iniciales (6 meses): {filas_iniciales}")

    # Cambiar filtro a "Todos"
    page.select_option("#date", "all")

    # Esperar que la tabla se recargue — el número de filas debe cambiar
    for intento in range(60):
        page.wait_for_timeout(1000)
        filas_ahora = page.query_selector_all("#grid-presentations tbody tr")
        n = len(filas_ahora)
        if n > 1 and n != filas_iniciales:
            print(f"  Tabla recargada en intento {intento+1}: {n} filas")
            break
        if intento % 5 == 0:
            print(f"  Esperando recarga... intento {intento+1}, filas: {n}")
    else:
        print("  ADVERTENCIA: tabla puede no haberse recargado, continuando igual")

    # Recorrer todas las páginas
    while True:
        filas = page.query_selector_all("#grid-presentations tbody tr")
        print(f"  Filas en página actual: {len(filas)}")

        for fila in filas:
            celdas = fila.query_selector_all("td")
            if len(celdas) < 7:
                continue
            span = celdas[3].query_selector("span[style*='font-weight']")
            nombre_form = span.inner_text().strip() if span else celdas[3].inner_text().strip()
            fecha_str   = celdas[1].inner_text().strip()
            estado      = celdas[6].inner_text().strip()
            try:
                fecha = datetime.strptime(fecha_str, "%d-%m-%Y").date()
            except ValueError:
                continue
            presentaciones.append({
                "nombre": nombre_form.upper(),
                "fecha":  fecha,
                "estado": estado,
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

    nombres_unicos = sorted(set(p["nombre"] for p in presentaciones))
    print("=== NOMBRES EXTRAÍDOS DE LA AIF ===")
    for n in nombres_unicos:
        print(f"  '{n}'")
    print("===================================")

    return presentaciones


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

            obligaciones = datos_alyc if tipo == "ALyC" else datos_an
            ws_oblig     = ws_alyc if tipo == "ALyC" else ws_an
            conteo = {"total": 0, "cumplidas": 0, "proximas": 0, "vencidas": 0}
            actualizaciones = []

            for i, fila in enumerate(obligaciones[8:]):
                if not fila or not fila[1]:
                    continue
                if fila[1].startswith("▶"):
                    continue
                if len(fila) > 7 and fila[7].strip() == "N/A":
                    continue

                codigo      = fila[1].strip()
                descripcion = fila[2].strip() if len(fila) > 2 else ""
                plazo_str   = fila[5].strip() if len(fila) > 5 else ""
                plazo_dias  = int(plazo_str) if plazo_str.isdigit() else None

                match = next(
                    (p for p in presentaciones
                     if NOMBRE_A_CODIGO.get(p["nombre"]) == codigo),
                    None
                )
                fecha_pres   = match["fecha"] if match else None
                estado_nuevo = calcular_estado(fecha_pres, plazo_dias)
                estado_ant   = fila[7].strip() if len(fila) > 7 else ""

                conteo["total"] += 1
                if estado_nuevo == "CUMPLIDO":
                    conteo["cumplidas"] += 1
                elif estado_nuevo == "PRÓXIMO":
                    conteo["proximas"] += 1
                else:
                    conteo["vencidas"] += 1

                if estado_nuevo != estado_ant:
                    row_num = i + 9
                    actualizaciones.append({
                        "row":         row_num,
                        "fecha":       fecha_pres,
                        "estado":      estado_nuevo,
                        "codigo":      codigo,
                        "descripcion": descripcion,
                        "estado_ant":  estado_ant,
                    })

            for upd in actualizaciones:
                ws_oblig.update_cell(upd["row"], 7,
                    upd["fecha"].strftime("%d/%m/%Y") if upd["fecha"] else "")
                time.sleep(1)
                ws_oblig.update_cell(upd["row"], 8, upd["estado"])
                time.sleep(1)
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
