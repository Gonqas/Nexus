from __future__ import annotations


def build_app_stylesheet() -> str:
    return """
    QWidget {
        color: #1f2933;
        font-family: "Segoe UI", "Trebuchet MS", sans-serif;
        font-size: 13px;
    }

    QMainWindow,
    QWidget#AppRoot,
    QWidget#ContentArea,
    QWidget#PageScrollContainer {
        background-color: #f4f1eb;
    }

    QFrame#Sidebar {
        background-color: #223036;
        border-right: 1px solid #30424a;
    }

    QLabel {
        background: transparent;
    }

    QLabel#BrandEyebrow {
        color: #e9d8bf;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 1px;
        text-transform: uppercase;
    }

    QLabel#BrandTitle {
        color: #f8fbfc;
        font-size: 28px;
        font-weight: 700;
    }

    QLabel#BrandSubtitle,
    QLabel#SidebarFooter {
        color: #c8d5d9;
        font-size: 13px;
    }

    QFrame#SidebarCard,
    QFrame#ShellHero,
    QFrame#ShellMetricCard,
    QWidget#MetricCard,
    QWidget#HeroPanel,
    QGroupBox,
    QGroupBox#DataPanel {
        background-color: #ffffff;
        border: 1px solid #ddd4c7;
        border-radius: 14px;
    }

    QLabel#ShellTitle,
    QLabel#PageTitle {
        font-size: 28px;
        font-weight: 700;
        color: #1f2933;
    }

    QLabel#ShellSubtitle,
    QLabel#PageSubtitle,
    QLabel#HeroSummary,
    QLabel#MetricDetail,
    QLabel#ShellHint {
        color: #66707a;
        font-size: 13px;
    }

    QLabel#HeroKicker,
    QLabel#NavSection,
    QLabel#MetricTitle,
    QLabel#ShellMetricTitle,
    QLabel#SectionLabel {
        color: #7a5a44;
        font-weight: 700;
    }

    QLabel#SectionLabel {
        font-size: 16px;
        margin-top: 8px;
    }

    QLabel#MetricValue,
    QLabel#ShellMetricValue {
        color: #111827;
        font-size: 27px;
        font-weight: 700;
    }

    QPushButton {
        background-color: #c6673c;
        color: #ffffff;
        border: none;
        border-radius: 10px;
        padding: 10px 14px;
        font-weight: 700;
    }

    QPushButton:hover {
        background-color: #b65c35;
    }

    QPushButton:pressed {
        background-color: #9b4b29;
    }

    QPushButton#NavButton {
        background: transparent;
        color: #eef4f6;
        text-align: left;
        padding: 11px 12px;
        border-radius: 10px;
        font-weight: 600;
    }

    QPushButton#NavButton:hover {
        background-color: rgba(255, 255, 255, 0.08);
    }

    QPushButton#NavButton:checked {
        background-color: #fff1e4;
        color: #8b4a2f;
    }

    QLabel#NavSection {
        color: #9fb4ba;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 12px;
    }

    QLabel#TopBadge {
        background-color: #f3e5d6;
        color: #8d5336;
        border: 1px solid #e6d3bf;
        border-radius: 10px;
        padding: 6px 10px;
        font-size: 12px;
        font-weight: 700;
    }

    QLineEdit,
    QTextEdit,
    QComboBox,
    QTableWidget,
    QProgressBar {
        background-color: #ffffff;
        border: 1px solid #d7cdbf;
        border-radius: 10px;
    }

    QLineEdit,
    QTextEdit,
    QComboBox {
        padding: 8px 10px;
    }

    QComboBox::drop-down {
        border: none;
        width: 26px;
    }

    QGroupBox {
        margin-top: 10px;
        padding-top: 14px;
        font-weight: 700;
        color: #3e3228;
    }

    QGroupBox::title {
        subcontrol-origin: margin;
        left: 12px;
        padding: 0 6px;
    }

    QHeaderView::section {
        background-color: #f2e5d4;
        color: #5b4637;
        border: none;
        border-right: 1px solid #e4d6c5;
        border-bottom: 1px solid #e4d6c5;
        padding: 8px;
        font-weight: 700;
    }

    QTableWidget {
        gridline-color: #ebdfd0;
        alternate-background-color: #faf7f2;
        selection-background-color: #fde7d8;
        selection-color: #1f2933;
    }

    QAbstractItemView {
        selection-background-color: #fde7d8;
        selection-color: #1f2933;
    }

    QProgressBar {
        text-align: center;
        min-height: 20px;
    }

    QProgressBar::chunk {
        background-color: #d77b4d;
        border-radius: 8px;
    }

    QScrollArea,
    QStackedWidget {
        border: none;
        background: transparent;
    }

    QSplitter::handle {
        background: transparent;
        width: 6px;
        height: 6px;
    }
    """
