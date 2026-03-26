from __future__ import annotations


def build_app_stylesheet() -> str:
    return """
    QWidget {
        color: #17212b;
        font-family: "Segoe UI Variable Text", "Segoe UI", sans-serif;
        font-size: 13px;
    }

    QMainWindow,
    QWidget#AppRoot,
    QWidget#ContentArea,
    QWidget#PageScrollContainer {
        background-color: #f6f7f9;
    }

    QLabel {
        background: transparent;
    }

    QFrame#Sidebar {
        background-color: #ffffff;
        border-right: 1px solid #dde3ea;
    }

    QLabel#BrandEyebrow {
        color: #58708b;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.8px;
        text-transform: uppercase;
    }

    QLabel#BrandTitle {
        color: #17212b;
        font-size: 24px;
        font-weight: 700;
    }

    QLabel#BrandSubtitle,
    QLabel#SidebarFooter,
    QLabel#ShellSubtitle,
    QLabel#PageSubtitle,
    QLabel#HeroSummary,
    QLabel#MetricDetail,
    QLabel#ShellHint {
        color: #5b6b7c;
        font-size: 13px;
    }

    QLabel#NavSection,
    QLabel#MetricTitle,
    QLabel#ShellMetricTitle,
    QLabel#SectionLabel,
    QLabel#HeroKicker {
        color: #39536d;
        font-weight: 700;
    }

    QLabel#SectionLabel {
        font-size: 15px;
        margin-top: 6px;
    }

    QLabel#ShellTitle,
    QLabel#PageTitle {
        font-size: 26px;
        font-weight: 700;
        color: #17212b;
    }

    QLabel#MetricValue,
    QLabel#ShellMetricValue {
        color: #0f1720;
        font-size: 26px;
        font-weight: 700;
    }

    QFrame#SidebarCard,
    QFrame#ShellHero,
    QFrame#ShellMetricCard,
    QWidget#MetricCard,
    QWidget#HeroPanel,
    QGroupBox,
    QGroupBox#DataPanel {
        background-color: #ffffff;
        border: 1px solid #dde3ea;
        border-radius: 14px;
    }

    QPushButton {
        background-color: #2f6fed;
        color: #ffffff;
        border: none;
        border-radius: 10px;
        padding: 9px 14px;
        font-weight: 600;
    }

    QPushButton:hover {
        background-color: #255fd0;
    }

    QPushButton:pressed {
        background-color: #1f53b8;
    }

    QPushButton:disabled {
        background-color: #cdd6e1;
        color: #ffffff;
    }

    QPushButton#NavButton {
        background: transparent;
        color: #314151;
        text-align: left;
        padding: 10px 12px;
        border-radius: 10px;
        font-weight: 600;
    }

    QPushButton#NavButton:hover {
        background-color: #eef4fb;
        color: #17365d;
    }

    QPushButton#NavButton:checked {
        background-color: #e8f0ff;
        color: #1f4fa3;
    }

    QPushButton#GhostButton {
        background-color: #ffffff;
        color: #234b91;
        border: 1px solid #c8d6ea;
    }

    QPushButton#GhostButton:hover {
        background-color: #f4f8ff;
    }

    QLabel#NavSection {
        color: #708295;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 10px;
    }

    QLabel#TopBadge {
        background-color: #eef4fb;
        color: #315f9f;
        border: 1px solid #d5e2f4;
        border-radius: 10px;
        padding: 5px 9px;
        font-size: 12px;
        font-weight: 700;
    }

    QLineEdit,
    QTextEdit,
    QComboBox,
    QTableWidget,
    QProgressBar,
    QAbstractScrollArea {
        background-color: #ffffff;
        border: 1px solid #d7e0ea;
        border-radius: 10px;
    }

    QLineEdit,
    QTextEdit,
    QComboBox {
        padding: 8px 10px;
    }

    QLineEdit:focus,
    QTextEdit:focus,
    QComboBox:focus {
        border: 1px solid #6b9cf0;
    }

    QComboBox::drop-down {
        border: none;
        width: 24px;
    }

    QGroupBox {
        margin-top: 9px;
        padding-top: 12px;
        font-weight: 700;
        color: #243447;
    }

    QGroupBox::title {
        subcontrol-origin: margin;
        left: 12px;
        padding: 0 6px;
    }

    QHeaderView::section {
        background-color: #f3f6fa;
        color: #425568;
        border: none;
        border-right: 1px solid #e0e6ee;
        border-bottom: 1px solid #e0e6ee;
        padding: 8px;
        font-weight: 700;
    }

    QTableCornerButton::section {
        background-color: #f3f6fa;
        border: none;
        border-right: 1px solid #e0e6ee;
        border-bottom: 1px solid #e0e6ee;
    }

    QTableWidget {
        gridline-color: #edf1f5;
        alternate-background-color: #fafbfd;
        selection-background-color: #e7f0ff;
        selection-color: #17212b;
    }

    QAbstractItemView {
        selection-background-color: #e7f0ff;
        selection-color: #17212b;
    }

    QProgressBar {
        text-align: center;
        min-height: 20px;
    }

    QProgressBar::chunk {
        background-color: #4f88f3;
        border-radius: 8px;
    }

    QCheckBox {
        spacing: 8px;
    }

    QCheckBox::indicator {
        width: 18px;
        height: 18px;
        border-radius: 5px;
        border: 1px solid #c8d6ea;
        background-color: #ffffff;
    }

    QCheckBox::indicator:checked {
        background-color: #2f6fed;
        border: 1px solid #2f6fed;
    }

    QScrollArea,
    QStackedWidget {
        border: none;
        background: transparent;
    }

    QTabWidget::pane {
        border: 1px solid #dde3ea;
        background: #ffffff;
        border-radius: 14px;
        top: -1px;
    }

    QTabBar::tab {
        background: #eef3f8;
        color: #5b6b7c;
        padding: 8px 14px;
        margin-right: 6px;
        border-top-left-radius: 10px;
        border-top-right-radius: 10px;
        font-weight: 600;
    }

    QTabBar::tab:selected {
        background: #ffffff;
        color: #17212b;
    }

    QTabBar::tab:hover:!selected {
        background: #f5f8fb;
    }

    QSplitter::handle {
        background: transparent;
        width: 6px;
        height: 6px;
    }

    QScrollBar:vertical {
        background: transparent;
        width: 10px;
        margin: 0;
    }

    QScrollBar::handle:vertical {
        background: #ced8e3;
        border-radius: 5px;
        min-height: 30px;
    }

    QScrollBar:horizontal {
        background: transparent;
        height: 10px;
        margin: 0;
    }

    QScrollBar::handle:horizontal {
        background: #ced8e3;
        border-radius: 5px;
        min-width: 30px;
    }

    QScrollBar::add-line,
    QScrollBar::sub-line,
    QScrollBar::add-page,
    QScrollBar::sub-page {
        background: transparent;
        border: none;
    }
    """
