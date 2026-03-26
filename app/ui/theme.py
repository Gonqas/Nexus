from __future__ import annotations


def build_app_stylesheet() -> str:
    return """
    QWidget {
        background: #f7f1e8;
        color: #1f1d1a;
        font-family: "Segoe UI", "Trebuchet MS", sans-serif;
        font-size: 13px;
    }

    QMainWindow {
        background: #f7f1e8;
    }

    QFrame#Sidebar {
        background: #1f2a2a;
        border-right: 1px solid #304343;
    }

    QLabel#BrandEyebrow {
        color: #d7c1a2;
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 1px;
        text-transform: uppercase;
    }

    QLabel#BrandTitle {
        color: #fff8ef;
        font-size: 26px;
        font-weight: 700;
    }

    QLabel#BrandSubtitle {
        color: #c3d0ce;
        font-size: 13px;
    }

    QFrame#SidebarCard,
    QWidget#HeroPanel,
    QWidget#MetricCard,
    QGroupBox#DataPanel,
    QFrame#ShellHero,
    QFrame#ShellMetricCard {
        background: #fffaf3;
        border: 1px solid #e0d4c1;
        border-radius: 16px;
    }

    QLabel#HeroKicker {
        color: #a24f2e;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 1px;
        text-transform: uppercase;
    }

    QLabel#PageTitle,
    QLabel#ShellTitle {
        font-size: 30px;
        font-weight: 700;
        color: #231f1a;
    }

    QLabel#PageSubtitle,
    QLabel#ShellSubtitle,
    QLabel#HeroSummary,
    QLabel#MetricDetail,
    QLabel#ShellHint {
        color: #6b6258;
    }

    QLabel#SectionLabel {
        font-size: 16px;
        font-weight: 700;
        color: #513a2b;
        margin-top: 8px;
    }

    QLabel#MetricTitle,
    QLabel#ShellMetricTitle {
        color: #7d6a58;
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
    }

    QLabel#MetricValue,
    QLabel#ShellMetricValue {
        color: #1f1d1a;
        font-size: 28px;
        font-weight: 700;
    }

    QPushButton {
        background: #c5643b;
        color: #fffaf3;
        border: none;
        border-radius: 10px;
        padding: 10px 14px;
        font-weight: 600;
    }

    QPushButton:hover {
        background: #af5631;
    }

    QPushButton:pressed {
        background: #944627;
    }

    QPushButton#NavButton {
        background: transparent;
        color: #d8e1df;
        text-align: left;
        padding: 12px 14px;
        border-radius: 12px;
        font-weight: 600;
    }

    QPushButton#NavButton:hover {
        background: rgba(255, 248, 239, 0.08);
    }

    QPushButton#NavButton:checked {
        background: #fff0df;
        color: #8e4728;
    }

    QLabel#NavSection {
        color: #92a7a3;
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 12px;
    }

    QLabel#SidebarFooter {
        color: #9cb2ae;
        font-size: 12px;
    }

    QLineEdit,
    QTextEdit,
    QComboBox,
    QTableWidget,
    QProgressBar,
    QAbstractItemView {
        background: #fffdf9;
        border: 1px solid #dbcdb9;
        border-radius: 10px;
        selection-background-color: #f0d7c5;
        selection-color: #2a211a;
    }

    QComboBox,
    QLineEdit,
    QTextEdit {
        padding: 8px 10px;
    }

    QGroupBox {
        margin-top: 10px;
        padding-top: 16px;
        font-weight: 700;
        color: #4c3b2e;
    }

    QGroupBox::title {
        subcontrol-origin: margin;
        left: 12px;
        padding: 0 6px;
    }

    QHeaderView::section {
        background: #efe1cf;
        color: #5b493a;
        border: none;
        border-right: 1px solid #e2d3bf;
        border-bottom: 1px solid #e2d3bf;
        padding: 8px;
        font-weight: 700;
    }

    QTableWidget {
        gridline-color: #eadbc8;
        alternate-background-color: #fbf5ee;
    }

    QScrollArea {
        border: none;
        background: transparent;
    }

    QTabWidget::pane {
        border: none;
    }

    QLabel#TopBadge {
        background: #efe1cf;
        color: #7d4d34;
        border-radius: 10px;
        padding: 6px 10px;
        font-size: 12px;
        font-weight: 700;
    }
    """
