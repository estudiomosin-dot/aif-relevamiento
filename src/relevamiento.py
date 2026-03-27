def obtener_o_crear_carpeta(drive_service, nombre_carpeta, parent_id=None):
    """Busca una carpeta por nombre. Si no existe, la crea."""
    query = f"name='{nombre_carpeta}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    results = drive_service.files().list(
        q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if files:
        return files[0]["id"]

    # No existe, crear
    metadata = {
        "name": nombre_carpeta,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        metadata["parents"] = [parent_id]

    folder = drive_service.files().create(
        body=metadata, fields="id").execute()
    print(f"  [DRIVE] Carpeta creada: {nombre_carpeta}")
    return folder.get("id")


def exportar_pdf_y_subir_drive(drive_service, sheet_id, gid,
                                nombre_archivo, nombre_cliente, tipo):
    """
    Exporta una pestaña del Sheet como PDF y la sube a Drive
    dentro de AIF Relevamientos / NOMBRE CLIENTE (TIPO).
    Retorna el file_id del PDF subido.
    """
    # URL de export
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export"
        f"?format=pdf"
        f"&gid={gid}"
        f"&portrait=false"
        f"&fitw=true"
        f"&gridlines=false"
        f"&printtitle=false"
        f"&sheetnames=false"
        f"&fzr=false"
    )

    # Token del service account
    from google.auth.transport.requests import Request
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    creds.refresh(Request())

    # Descargar PDF
    headers = {"Authorization": f"Bearer {creds.token}"}
    response = requests.get(export_url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            f"Error exportando PDF: {response.status_code} — {response.text[:200]}")

    pdf_bytes = response.content
    print(f"  [PDF] Descargado: {len(pdf_bytes)} bytes")

    # Estructura de carpetas: AIF Relevamientos / NOMBRE (TIPO)
    carpeta_raiz   = obtener_o_crear_carpeta(drive_service, "AIF Relevamientos")
    carpeta_cliente = obtener_o_crear_carpeta(
        drive_service,
        f"{nombre_cliente} ({tipo})",
        parent_id=carpeta_raiz
    )

    # Subir PDF a la carpeta del cliente
    file_metadata = {
        "name": f"{nombre_archivo}.pdf",
        "mimeType": "application/pdf",
        "parents": [carpeta_cliente],
    }
    media = MediaIoBaseUpload(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        resumable=False
    )
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    file_id = file.get("id")

    # Permiso de lectura para que Make pueda descargarlo
    drive_service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
    ).execute()

    print(f"  [PDF] Subido: AIF Relevamientos/{nombre_cliente} ({tipo})/{nombre_archivo}.pdf")
    print(f"  [PDF] File ID: {file_id}")
    return file_id
