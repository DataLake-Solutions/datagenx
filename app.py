import base64
import concurrent.futures
import json
import html
import re
import time
from pathlib import Path

import streamlit as st

from data_generation_backend import (
    create_schema,
    ensure_storage,
    generate_schema_data,
    get_schema,
    list_schemas,
    remove_table,
    update_schema_prompt,
    upsert_schema_table,
)


st.set_page_config(page_title="Data Gen", layout="wide")
ensure_storage()
ASSETS_DIR = Path(__file__).resolve().parent / "assets"


def _status_color(status: str) -> str:
    status = (status or "").upper()
    if status == "DONE":
        return "#1e8e3e"
    if status == "ERROR":
        return "#d93025"
    if status == "INPROGRESS":
        return "#1a73e8"
    return "#5f6368"


def _status_visual(status: str) -> tuple[str, str, str]:
    s = (status or "").upper()
    if s == "DONE":
        return "step-done", "OK", "DONE"
    if s in {"INPROGRESS", "IN_PROGRESS", "UPLOADING", "RUNNING", "PROCESSING", "PENDING"}:
        return "step-processing", "<span class='spin'></span>", "PROCESSING"
    if s == "ERROR":
        return "step-error", "!", "ERROR"
    return "step-new", ".", (s or "NEW")


def _render_styles() -> None:
    st.markdown(
        """
        <style>
        #MainMenu, footer, header { visibility: hidden; }
        [data-testid="stAppViewContainer"] { background: #eceef3; }
        [data-testid="stMainBlockContainer"] {
            max-width: 1650px;
            padding-top: 7rem;
            padding-left: 2.2rem;
            padding-right: 2.2rem;
        }
        .topbar {
            position: fixed; left: 0; right: 0; top: 0; z-index: 9999;
            height: 86px; background: #000b4f; color: white;
            display: flex; align-items: center; justify-content: space-between;
            padding: 0 26px; box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }
        .brand { font-size: 22px; font-weight: 900; letter-spacing: 1px; display:flex; align-items:center; gap: 12px; }
        .brand-icon { width: 56px; height: 56px; object-fit: contain; filter: drop-shadow(0 0 1px rgba(255,255,255,0.45)); }
        .brand-text { font-size: 24px; font-weight: 700; }
        .nav-links { display: flex; gap: 70px; font-size: 20px; font-weight: 700; opacity: .95; }
        .nav-item { display:flex; align-items:center; gap: 10px; }
        .nav-icon { width: 28px; height: 28px; object-fit: contain; filter: brightness(0) invert(1); }
        .logout-pill { background: #d8d100; color: #101010; border-radius: 30px; padding: 12px 34px; font-size: 26px; font-weight: 700; }
        .page-footer {
            margin-top: 22px;
            text-align: center;
            font-size: 16px;
            color: #334155;
            padding: 8px 0 16px 0;
        }

        [data-testid="stVerticalBlockBorderWrapper"] {
            border: 0 !important;
            border-radius: 0 !important;
            background: transparent !important;
            box-shadow: none !important;
            padding: 0 !important;
            margin-bottom: 0 !important;
        }
        .st-key-top_section,
        .st-key-tables_section,
        [class*="st-key-table_card_"] {
            border: 1px solid #d6dae3 !important;
            border-radius: 24px !important;
            background: #f7f8fb !important;
            box-shadow: 0 8px 18px rgba(12, 24, 56, 0.05) !important;
            padding: 12px !important;
            margin-bottom: 14px !important;
        }
        [class*="st-key-table_card_"] {
            border-radius: 18px !important;
            background: #fafbfe !important;
            box-shadow: 0 4px 12px rgba(12, 24, 56, 0.04) !important;
            margin-top: 8px !important;
        }

        .section-title { font-size: 18px; font-weight: 800; color: #0f1d43; margin: 6px 0 10px 0; }
        .lead-text { font-size: 18px; color: #081b46; line-height: 1.5; margin-bottom: 12px; }
        .stage-line { height: 4px; border-radius: 10px; background: #8755ff; margin: 8px 0 16px 0; }
        .stage-card { border-radius: 18px; background: #e5e6ea; border: 1px solid #dadbe0; padding: 18px; min-height: 130px; }
        .stage-step { font-size: 16px; color: #5e6e84; }
        .stage-name { font-size: 18px; font-weight: 800; color: #0f1d43; line-height: 1.2; margin-top: 8px; }
        .meta-note { font-size: 16px; color: #6a7283; margin-top: 6px; }
        .table-head { font-size: 20px; font-weight: 700; color: #4a89d6; margin-bottom: 8px; }
        .table-sub { font-size: 20px; color: #0f1d43; }
        .custom-caption { font-size: 17px; color: #5f6b80; }

        [data-testid="stSelectbox"] label { display:none; }
        [data-testid="stSelectbox"] > div > div {
            border-radius: 16px !important;
            border: 2px solid #9ea6b8;
            background: #f5f6f8;
            min-height: 46px;
        }
        [data-baseweb="select"] * { color: #0f1d43 !important; }
        [data-baseweb="select"] svg { color: #0f1d43 !important; fill: #0f1d43 !important; opacity: 1 !important; }
        [data-baseweb="select"] input { color: #0f1d43 !important; -webkit-text-fill-color: #0f1d43 !important; }

        [data-testid="stTextArea"] textarea {
            border-radius: 16px !important;
            border: 1px solid #b6bcc8 !important;
            font-family: Consolas, monospace !important;
            background: #ffffff !important;
            color: #0f1d43 !important;
            -webkit-text-fill-color: #0f1d43 !important;
            caret-color: #0f1d43 !important;
        }
        [data-testid="stTextArea"] textarea::placeholder {
            color: #7b8395 !important;
            opacity: 1 !important;
        }
        .st-key-schema_prompt_input textarea,
        [class*="st-key-schema_prompt_input_"] textarea {
            resize: vertical !important;
            min-height: 170px !important;
            max-height: 70vh !important;
            overflow: auto !important;
        }
        [data-testid="stTextInput"] label,
        [data-testid="stNumberInput"] label {
            color: #0f1d43 !important;
            opacity: 1 !important;
            font-weight: 700 !important;
        }
        [data-testid="stTextInput"] input,
        [data-testid="stNumberInput"] input {
            background: #ffffff !important;
            color: #0f1d43 !important;
            -webkit-text-fill-color: #0f1d43 !important;
            border: 1px solid #c9d3e5 !important;
            caret-color: #0f1d43 !important;
            border-radius: 12px !important;
        }
        [data-testid="stTextInput"] input::placeholder,
        [data-testid="stNumberInput"] input::placeholder {
            color: #7b8395 !important;
            opacity: 1 !important;
        }
        [data-testid="stNumberInput"] button {
            background: #f5f6f8 !important;
            border: 1px solid #c9d3e5 !important;
            color: #0f1d43 !important;
        }
        [data-testid="stFormSubmitButton"] button {
            background: #3b82f6 !important;
            border: 1px solid #3b82f6 !important;
            color: #ffffff !important;
            border-radius: 12px !important;
            font-weight: 700 !important;
        }

        .code-panel {
            border: 1px solid #aeb4bf;
            border-radius: 14px;
            background: #ffffff;
            min-height: 250px;
            max-height: 250px;
            overflow: auto;
            font-family: Consolas, monospace;
            font-size: 17px;
            line-height: 1.5;
            padding: 12px 0;
        }
        .code-row {
            display: grid;
            grid-template-columns: 52px 1fr;
            white-space: pre;
            padding: 0 14px 0 8px;
        }
        .code-ln {
            color: #6b778c;
            text-align: right;
            padding-right: 14px;
            user-select: none;
        }
        .code-txt { color: #0e111a; }
        .code-kw { color: #3b82c4; font-weight: 500; }
        .code-num { color: #3b82c4; }
        .code-tbl { color: #7c3aed; font-weight: 600; }
        .code-col { color: #0f766e; }
        .code-type { color: #b45309; font-weight: 600; }
        .code-sel { color: #2563eb; font-weight: 700; }

        .st-key-btn_add_schema button {
            background: #aec8e9 !important;
            border: 1px solid #aec8e9 !important;
            color: #111f4a !important;
            font-weight: 700 !important;
            border-radius: 12px !important;
            height: 46px !important;
            margin-top: 0 !important;
        }
        .st-key-btn_add_schema button::before { content: "+ "; color: #8c63ff; }
        .st-key-add_schema_name_input input {
            background: #ffffff !important;
            border: 1px solid #c9d3e5 !important;
            color: #0f1d43 !important;
            -webkit-text-fill-color: #0f1d43 !important;
            caret-color: #0f1d43 !important;
        }
        .st-key-add_schema_name_input input:focus {
            border: 1px solid #6b9de8 !important;
            box-shadow: 0 0 0 2px rgba(107,157,232,0.18) !important;
        }
        .st-key-add_schema_name_input {
            margin-top: 0 !important;
        }
        [data-testid="stForm"] {
            border: 0 !important;
            padding: 0 !important;
            margin: 0 !important;
            background: transparent !important;
        }
        [data-testid="stForm"] > div {
            padding: 0 !important;
            margin: 0 !important;
        }
        .st-key-btn_create_schema button {
            background: #3b82f6 !important;
            border: 1px solid #3b82f6 !important;
            color: #ffffff !important;
            font-weight: 700 !important;
            border-radius: 10px !important;
        }

        .st-key-btn_collapse_top button {
            border-radius: 999px !important;
            width: 46px !important;
            height: 46px !important;
            min-height: 46px !important;
            padding: 0 !important;
            background: #aec8e9 !important;
            border: 1px solid #aec8e9 !important;
            color: #111f4a !important;
            font-size: 24px !important;
            font-weight: 800 !important;
            margin-top: 0 !important;
        }

        .st-key-btn_update_schema button,
        .st-key-btn_generate button,
        .st-key-btn_add_table button,
        [class*="st-key-edit_"] button,
        [class*="st-key-btn_view_validation_"] button {
            background: #aec8e9 !important;
            border: 1px solid #aec8e9 !important;
            color: #111f4a !important;
            font-weight: 700 !important;
            border-radius: 18px !important;
        }
        [class*="st-key-remove_"] button {
            background: #fff6f7 !important;
            border: 1px solid #efb5ba !important;
            color: #b32228 !important;
            font-weight: 700 !important;
            border-radius: 18px !important;
        }

        .status-pill {
            display: inline-block; padding: 4px 10px; border-radius: 14px;
            color: #fff; font-weight: 700; font-size: 14px;
        }
        .step-status-row { display: flex; align-items: center; gap: 8px; margin-top: 10px; }
        .step-icon {
            width: 22px; height: 22px; border-radius: 999px;
            display: inline-flex; align-items: center; justify-content: center;
            font-size: 15px; font-weight: 800;
        }
        .step-done { background: #2ea44f; color: #fff; }
        .step-processing { background: #1a73e8; color: #fff; }
        .step-new { background: #6b7280; color: #fff; }
        .step-error { background: #d93025; color: #fff; }
        .spin {
            width: 12px; height: 12px; border-radius: 999px;
            border: 2px solid rgba(255,255,255,0.35); border-top-color: #fff;
            animation: spin 0.9s linear infinite;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
        .spacer-sm { height: 6px; }
        .spacer-md { height: 16px; }

        /* Validation report dialog sizing + blue theme */
        [data-testid="stDialog"] [role="dialog"] {
            width: min(1000px, 92vw) !important;
            max-width: min(1000px, 92vw) !important;
        }
        [data-testid="stDialog"] [role="dialog"] > div {
            background: #ffffff !important;
            border: 1px solid #d7dee9 !important;
            border-radius: 14px !important;
        }
        [data-testid="stDialog"] [role="dialog"] input,
        [data-testid="stDialog"] [role="dialog"] textarea {
            background: #ffffff !important;
            color: #0f1d43 !important;
            -webkit-text-fill-color: #0f1d43 !important;
            border: 1px solid #c2d2e8 !important;
        }
        [data-testid="stDialog"] [role="dialog"] textarea {
            resize: vertical !important;
            overflow: auto !important;
            min-height: 120px !important;
            max-height: 70vh !important;
        }
        [data-testid="stDialog"] textarea {
            resize: both !important;
            overflow: auto !important;
        }
        [data-testid="stDialog"] textarea::-webkit-resizer {
            background: #b7c9e8 !important;
            border-radius: 4px !important;
        }
        [data-testid="stDialog"] [role="dialog"] label {
            color: #4a89d6 !important;
            font-weight: 700 !important;
            opacity: 1 !important;
        }
        .modal-title {
            font-size: 40px;
            font-weight: 800;
            color: #4a89d6;
            margin: 2px 0 4px 0;
        }
        .modal-divider {
            height: 1px;
            background: #d7e3f3;
            margin: 8px 0 12px 0;
        }
        .st-key-btn_modal_submit button {
            background: #aec8e9 !important;
            border: 1px solid #aec8e9 !important;
            color: #0f1d43 !important;
            font-weight: 700 !important;
            border-radius: 999px !important;
        }
        .st-key-btn_modal_cancel button {
            background: transparent !important;
            border: 0 !important;
            color: #222 !important;
            font-weight: 500 !important;
            box-shadow: none !important;
        }
        [data-testid="stDialog"] [role="dialog"] h1,
        [data-testid="stDialog"] [role="dialog"] h2,
        [data-testid="stDialog"] [role="dialog"] h3 {
            color: #123a7a !important;
        }
        [data-testid="stDialog"] [role="dialog"] p,
        [data-testid="stDialog"] [role="dialog"] label,
        [data-testid="stDialog"] [role="dialog"] .stMarkdown {
            color: #123a7a !important;
        }
        [data-testid="stDialog"] [role="dialog"] [data-testid="stExpander"] {
            background: #ffffff !important;
            border: 1px solid #d4e2f7 !important;
            border-radius: 10px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _init_state() -> None:
    if "selected_schema" not in st.session_state:
        opts = list_schemas()
        st.session_state.selected_schema = opts[0]["value"] if opts else ""
    if "editing_table_id" not in st.session_state:
        st.session_state.editing_table_id = ""
    if "show_add_schema" not in st.session_state:
        st.session_state.show_add_schema = False
    if "last_generated" not in st.session_state:
        st.session_state.last_generated = {}
    if "workflow_collapsed" not in st.session_state:
        st.session_state.workflow_collapsed = False
    if "schema_update_message" not in st.session_state:
        st.session_state.schema_update_message = ""
    if "schema_update_error" not in st.session_state:
        st.session_state.schema_update_error = ""
    if "generation_error_by_schema" not in st.session_state:
        st.session_state.generation_error_by_schema = {}


def _asset_data_uri(filename: str) -> str:
    path = ASSETS_DIR / filename
    if not path.exists():
        return ""
    ext = path.suffix.lower()
    mime = "image/png" if ext == ".png" else "image/svg+xml"
    data = base64.b64encode(path.read_bytes()).decode("utf-8")
    return f"data:{mime};base64,{data}"


def _safe_schema_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", value).strip("_") or "schema"


def _validation_report_path(schema: dict) -> Path:
    schema_name = _safe_schema_name(schema.get("org_name", schema.get("org_id", "schema")))
    return Path(__file__).resolve().parent / "schemas" / schema_name / "validations" / "validation_report.json"


@st.dialog("Validation Report")
def _open_validation_report_dialog(report_data: object) -> None:
    if isinstance(report_data, dict):
        summary = str(report_data.get("summary", "")).strip() or "No summary provided."
        st.markdown(f"**Summary:** {summary}")

        tables = report_data.get("tables", [])
        if not isinstance(tables, list) or not tables:
            st.info("No table checks found in validation report.")
        else:
            for table in tables:
                table_name = str(table.get("table_name", "Unknown Table"))
                checks = table.get("checks", [])
                passed = sum(1 for c in checks if bool(c.get("passed", False))) if isinstance(checks, list) else 0
                total = len(checks) if isinstance(checks, list) else 0
                with st.expander(f"{table_name}  ({passed}/{total} passed)", expanded=True):
                    if not isinstance(checks, list) or not checks:
                        st.write("No checks available.")
                    else:
                        for chk in checks:
                            ok = bool(chk.get("passed", False))
                            icon = "[OK]" if ok else "[X]"
                            name = str(chk.get("name", "Unnamed check"))
                            details = str(chk.get("details", "")).strip()
                            st.markdown(f"{icon} **{name}**")
                            if details:
                                st.caption(details)

        with st.expander("Raw JSON"):
            st.code(json.dumps(report_data, indent=2), language="json")
    else:
        st.text_area("Validation report", value=str(report_data), height=430, label_visibility="collapsed")


def _schema_picker() -> None:
    options = list_schemas()
    id_to_label = {x["value"]: x["label"] for x in options}
    labels = [id_to_label[x["value"]] for x in options]
    ids = [x["value"] for x in options]

    c1, c2, c3 = st.columns([5, 1.3, 0.35], vertical_alignment="center")
    with c1:
        if not ids:
            st.warning("No schema exists yet. Add one.")
            st.session_state.selected_schema = ""
        else:
            current_idx = ids.index(st.session_state.selected_schema) if st.session_state.selected_schema in ids else 0
            picked_label = st.selectbox(
                "Select Schema",
                labels,
                index=current_idx,
                label_visibility="collapsed",
                key="schema_selectbox",
            )
            st.session_state.selected_schema = ids[labels.index(picked_label)]

    with c2:
        if st.button("Add New Schema", use_container_width=True, key="btn_add_schema"):
            st.session_state.show_add_schema = not st.session_state.show_add_schema

    with c3:
        collapse_icon = "v" if st.session_state.workflow_collapsed else "^"
        if st.button(collapse_icon, key="btn_collapse_top", help="Collapse/Expand workflow"):
            st.session_state.workflow_collapsed = not st.session_state.workflow_collapsed
            st.rerun()

    if st.session_state.show_add_schema:
        with st.form("add_schema_form", clear_on_submit=True):
            new_name = st.text_input(
                "New Schema Name",
                key="add_schema_name_input",
                label_visibility="collapsed",
                placeholder="New schema name",
            )
            submitted = st.form_submit_button("Create Schema", key="btn_create_schema")
            if submitted:
                try:
                    created = create_schema(new_name)
                    st.session_state.selected_schema = created["value"]
                    st.session_state.show_add_schema = False
                    st.success(f"Schema '{created['label']}' created.")
                    st.rerun()
                except Exception as exc:
                    st.error(str(exc))


def _schema_instructions(schema: dict) -> None:
    st.markdown("<div class='section-title'>Schema Instructions</div>", unsafe_allow_html=True)
    org_id = str(schema.get("org_id", ""))
    prompt_key = f"schema_prompt_input_{org_id}"

    prompt_value = st.text_area(
        "Schema Prompt",
        value=schema.get("schema_prompt", ""),
        height=170,
        label_visibility="collapsed",
        key=prompt_key,
    )
    c_btn, c_msg = st.columns([1, 4], vertical_alignment="center")
    with c_btn:
        if st.button("Update", key="btn_update_schema"):
            if not str(prompt_value).strip():
                st.session_state.schema_update_message = ""
                st.session_state.schema_update_error = "Please enter instructions."
            else:
                try:
                    update_schema_prompt(org_id, prompt_value)
                    st.session_state.schema_update_error = ""
                    st.session_state.schema_update_message = "Updated successfully"
                    st.rerun()
                except Exception as exc:
                    st.session_state.schema_update_message = ""
                    st.session_state.schema_update_error = str(exc)
    with c_msg:
        if st.session_state.schema_update_message:
            st.markdown(
                "<span style='color:#16a34a; font-weight:600;'>Updated successfully</span>",
                unsafe_allow_html=True,
            )
        elif st.session_state.schema_update_error:
            st.markdown(
                f"<span style='color:#dc2626; font-weight:600;'>{st.session_state.schema_update_error}</span>",
                unsafe_allow_html=True,
            )


def _render_steps(schema: dict, target=None, key_prefix: str = "static", show_report_button: bool = True) -> None:
    if target is None:
        ctx = st.container()
    else:
        ctx = target.container()

    with ctx:
        st.markdown("<div class='spacer-md'></div>", unsafe_allow_html=True)
        st.markdown("<div class='stage-line'></div>", unsafe_allow_html=True)
        steps = [
            ("Code Generation", schema.get("dg_code_gen_status"), schema.get("dg_code_gen_log", "")),
            ("Bulk Data Generation", schema.get("dg_bulkdata_gen_status"), schema.get("dg_bulkdata_gen_log", "")),
            ("Data Validations", schema.get("dg_sf_upload_status"), schema.get("dg_sf_upload_log", "")),
            ("Done", schema.get("schema_gen_status"), schema.get("schema_gen_log", "")),
        ]
        cols = st.columns(4, gap="small")
        overall_status = str(schema.get("schema_gen_status", "")).upper()
        is_overall_error = overall_status == "ERROR"
        processing_states = {"INPROGRESS", "IN_PROGRESS", "UPLOADING", "RUNNING", "PROCESSING", "PENDING"}
        for idx, (name, status, log) in enumerate(steps):
            with cols[idx]:
                status_display = str(status or "").upper()
                if is_overall_error and status_display in processing_states:
                    status_display = "ERROR"
                color = _status_color(status_display)
                icon_class, icon_html, label = _status_visual(status_display)
                st.markdown(
                    f"<div class='stage-card'>"
                    f"<div class='stage-step'>STEP {idx+1}</div>"
                    f"<div class='stage-name'>{name}</div>"
                    f"<div class='step-status-row'>"
                    f"<span class='step-icon {icon_class}'>{icon_html}</span>"
                    f"<span class='status-pill' style='background:{color}'>{label}</span>"
                    f"</div>"
                    f"<div class='meta-note'>{(log or '')[:120]}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
                if idx == 2 and show_report_button:
                    report_path = _validation_report_path(schema)
                    if report_path.exists():
                        s = str(status or "").upper()
                        if s == "DONE":
                            btn_key = f"{key_prefix}_btn_view_validation_done_{schema.get('org_id','')}"
                        elif s == "ERROR":
                            btn_key = f"{key_prefix}_btn_view_validation_error_{schema.get('org_id','')}"
                        else:
                            btn_key = f"{key_prefix}_btn_view_validation_{schema.get('org_id','')}"
                        if st.button("View Report", key=btn_key, use_container_width=True):
                            try:
                                report_obj = json.loads(report_path.read_text(encoding="utf-8"))
                                _open_validation_report_dialog(report_obj)
                            except Exception as exc:
                                _open_validation_report_dialog(f"Could not load report: {exc}")


def _generate_action(schema: dict, steps_slot=None) -> None:
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    org_id = schema["org_id"]
    if st.button("Generate Data", type="primary", key="btn_generate"):
        st.session_state.generation_error_by_schema[org_id] = ""
        progress_msg = st.empty()
        progress_bar = st.progress(0, text="Starting generation...")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(generate_schema_data, org_id)
                while not future.done():
                    snap = get_schema(org_id)
                    s1 = str(snap.get("dg_code_gen_status", "NEW")).upper()
                    s2 = str(snap.get("dg_bulkdata_gen_status", "NEW")).upper()
                    s3 = str(snap.get("dg_sf_upload_status", "NEW")).upper()
                    s4 = str(snap.get("schema_gen_status", "NEW")).upper()
                    done_count = sum(1 for s in [s1, s2, s3, s4] if s == "DONE")
                    pct = min(100, done_count * 25)
                    progress_bar.progress(
                        pct,
                        text=f"Step1:{s1} | Step2:{s2} | Step3:{s3} | Step4:{s4}",
                    )
                    progress_msg.caption("Generation is running. Status updates are live.")
                    if steps_slot is not None:
                        _render_steps(
                            snap,
                            target=steps_slot,
                            key_prefix="live",
                            show_report_button=False,
                        )
                    time.sleep(0.8)

                csv_paths, out_dir = future.result()

            st.session_state.last_generated[org_id] = {
                "files": [str(p) for p in csv_paths],
                "out_dir": str(out_dir),
            }
            st.session_state.generation_error_by_schema[org_id] = ""
            progress_bar.progress(100, text="All steps completed.")
            progress_msg.empty()
            st.success(f"Generation complete. Files saved at: {out_dir}")
            st.rerun()
        except Exception as exc:
            progress_msg.empty()
            progress_bar.empty()
            st.session_state.generation_error_by_schema[org_id] = f"Generation failed: {exc}"

    generation_error = st.session_state.generation_error_by_schema.get(org_id, "")
    if generation_error:
        st.error(generation_error)

    generated = st.session_state.last_generated.get(org_id)
    if generated:
        st.info("Generation completed successfully.")


def _table_form(existing: dict | None = None) -> None:
    existing = existing or {}
    is_edit = bool(existing)
    st.markdown(f"<div class='modal-title'>{'Edit Table' if is_edit else 'Add Table'}</div>", unsafe_allow_html=True)
    st.markdown("<div class='modal-divider'></div>", unsafe_allow_html=True)
    with st.form("table_form_modal", clear_on_submit=not is_edit):
        c1, c2 = st.columns([2, 1])
        with c1:
            table_name = st.text_input(
                "Table Name",
                value=existing.get("table_name", ""),
                placeholder="Table Name",
            )
        with c2:
            num_entries = st.number_input(
                "Records Count",
                min_value=1,
                step=1,
                value=int(existing.get("num_entries", 1000)),
            )
        ddl = st.text_area("DDL", value=existing.get("ddl", ""), height=120, placeholder="CREATE TABLE ...")
        instructions = st.text_area(
            "Instructions",
            value=existing.get("instructions", ""),
            height=120,
            placeholder="Add instructions for intelligent data generation",
        )
        c_add, c_cancel, _ = st.columns([1, 1, 6], vertical_alignment="center")
        with c_add:
            submitted = st.form_submit_button(
                "Update" if is_edit else "Add",
                key="btn_modal_submit",
                use_container_width=True,
            )
        with c_cancel:
            canceled = st.form_submit_button(
                "Cancel",
                key="btn_modal_cancel",
                use_container_width=True,
            )
        if canceled:
            st.session_state.editing_table_id = ""
            st.rerun()
        if submitted:
            payload = {
                "table_id": existing.get("table_id", ""),
                "table_name": table_name,
                "num_entries": int(num_entries),
                "ddl": ddl,
                "instructions": instructions,
                "columns_list": existing.get("columns_list", []),
            }
            try:
                upsert_schema_table(st.session_state.selected_schema, payload)
                st.session_state.editing_table_id = ""
                st.success("Table saved.")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))


@st.dialog(" ")
def _open_table_dialog(existing: dict | None = None) -> None:
    _table_form(existing)


def _format_code_html(text: str, mode: str = "plain") -> str:
    lines = (text or "").splitlines() or [""]
    sql_keywords = [
        "CREATE", "OR", "REPLACE", "TABLE", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "NOT", "NULL",
        "UNIQUE", "DEFAULT", "AS", "ON", "AND", "JOIN", "FROM", "WHERE", "GROUP", "BY", "ORDER",
        "HAVING", "LIMIT", "INSERT", "INTO", "UPDATE", "DELETE", "VALUES",
    ]
    sql_types = [
        "STRING", "VARCHAR", "CHAR", "TEXT",
        "NUMBER", "INTEGER", "INT", "BIGINT", "SMALLINT",
        "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE",
        "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "TIMESTAMP_TZ", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ",
    ]
    rows = []
    for idx, raw in enumerate(lines, start=1):
        escaped = html.escape(raw)
        if mode == "sql":
            escaped = re.sub(
                r"\bSELECT\b",
                "<span class='code-sel'>SELECT</span>",
                escaped,
                flags=re.IGNORECASE,
            )
            escaped = re.sub(
                r"\b(CREATE\s+(?:OR\s+REPLACE\s+)?TABLE|ALTER\s+TABLE|FROM|JOIN|INTO|UPDATE)\s+([A-Za-z_][A-Za-z0-9_$]*)",
                lambda m: f"<span class='code-kw'>{m.group(1)}</span> <span class='code-tbl'>{m.group(2)}</span>",
                escaped,
                flags=re.IGNORECASE,
            )
            escaped = re.sub(
                r"^(\s*)([A-Za-z_][A-Za-z0-9_$]*)(\s+)([A-Za-z_][A-Za-z0-9_]*)",
                lambda m: (
                    f"{m.group(1)}<span class='code-col'>{m.group(2)}</span>{m.group(3)}"
                    f"<span class='code-type'>{m.group(4)}</span>"
                ) if m.group(4).upper() in sql_types else m.group(0),
                escaped,
                flags=re.IGNORECASE,
            )
            for kw in sql_keywords:
                escaped = re.sub(rf"\\b{kw}\\b", f"<span class='code-kw'>{kw}</span>", escaped, flags=re.IGNORECASE)
            for dt in sql_types:
                escaped = re.sub(rf"\\b{dt}\\b", f"<span class='code-type'>{dt}</span>", escaped, flags=re.IGNORECASE)
        escaped = re.sub(r"\b(\d+)\b", r"<span class='code-num'>\1</span>", escaped)
        rows.append(f"<div class='code-row'><span class='code-ln'>{idx}</span><span class='code-txt'>{escaped}</span></div>")
    return "<div class='code-panel'>" + "".join(rows) + "</div>"


def _tables_section(schema: dict) -> None:
    st.markdown(
        "<div class='lead-text'>Manage your schema tables easily by adding, updating, or deleting entries. "
        "Each table includes details such as name, number of entries, DDL, and instructions.</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<div class='section-title'>Schema Tables</div>", unsafe_allow_html=True)
    tables = schema.get("schema_list", [])
    if not tables:
        st.info("No tables yet. Add your first table below.")

    if st.button("Add Table", key="btn_add_table"):
        st.session_state.editing_table_id = "__new__"
        st.rerun()

    for table in tables:
        with st.container(border=True, key=f"table_card_{table['table_id']}"):
            c1, c2, c3 = st.columns([3, 2, 2], vertical_alignment="center")
            c1.markdown(f"<div class='table-head'>Table Name: <span class='table-sub'>{table.get('table_name', '')}</span></div>", unsafe_allow_html=True)
            c2.markdown(f"<div class='table-head'>Records Count: <span class='table-sub'>{table.get('num_entries', 0)}</span></div>", unsafe_allow_html=True)
            with c3:
                e1, e2 = st.columns(2)
                if e1.button("Edit", key=f"edit_{table['table_id']}", use_container_width=True):
                    st.session_state.editing_table_id = table["table_id"]
                    st.rerun()
                if e2.button("Remove", key=f"remove_{table['table_id']}", use_container_width=True):
                    remove_table(schema["org_id"], table["table_id"])
                    if st.session_state.editing_table_id == table["table_id"]:
                        st.session_state.editing_table_id = ""
                    st.rerun()

            d1, d2 = st.columns(2)
            with d1:
                st.markdown(_format_code_html(table.get("ddl", "") or "", mode="sql"), unsafe_allow_html=True)
            with d2:
                st.markdown(_format_code_html(str(table.get("instructions", "")) or "", mode="plain"), unsafe_allow_html=True)

            st.markdown("<div class='custom-caption'>Fields expected: table_name, num_entries, ddl, instructions.</div>", unsafe_allow_html=True)

    edit_item = None
    if st.session_state.editing_table_id and st.session_state.editing_table_id != "__new__":
        edit_item = next((t for t in tables if t.get("table_id") == st.session_state.editing_table_id), None)
    if st.session_state.editing_table_id:
        _open_table_dialog(edit_item)


def _render_topbar() -> None:
    logo = _asset_data_uri("datagen_icon_1.png")
    query_icon = _asset_data_uri("query.png")
    gen_icon = _asset_data_uri("data-processing.png")
    st.markdown(
        f"""
        <div class="topbar">
          <div class="brand">
            <img src="{logo}" class="brand-icon" />
            <span class="brand-text">DATA GEN</span>
          </div>
          <div class="nav-links">
            <span class="nav-item"><img src="{query_icon}" class="nav-icon" /> Data Query</span>
            <span class="nav-item"><img src="{gen_icon}" class="nav-icon" /> Data Gen</span>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_footer() -> None:
    st.markdown(
        "<div class='page-footer'>Powered by Datalake Solutions. For information or support contact: info@datalake-solutions.com</div>",
        unsafe_allow_html=True,
    )


def main() -> None:
    _render_styles()
    _init_state()
    _render_topbar()

    with st.container(border=True, key="top_section"):
        st.markdown(
            "<div class='lead-text'>Select a schema from the dropdown to generate data and manage its structure. "
            "You can easily add, update, or modify schema tables as needed.</div>",
            unsafe_allow_html=True,
        )

        _schema_picker()
        if not st.session_state.selected_schema:
            st.stop()

        schema = get_schema(st.session_state.selected_schema)

        if not st.session_state.workflow_collapsed:
            _schema_instructions(schema)
            steps_slot = st.empty()
            _render_steps(schema, target=steps_slot, key_prefix="static")
            _generate_action(schema, steps_slot=steps_slot)

    with st.container(border=True, key="tables_section"):
        _tables_section(schema)
    _render_footer()

if __name__ == "__main__":
    main()








