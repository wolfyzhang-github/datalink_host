from __future__ import annotations

from functools import partial

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtGui, QtWidgets

from datalink_host.core.config import AppSettings
from datalink_host.core.logging import get_recent_logs
from datalink_host.models.messages import RuntimeSnapshot
from datalink_host.services.runtime import RuntimeService, downsample_for_plot, slice_for_plot


PROCESSING_QUEUE_CAPACITY = 32
STORAGE_QUEUE_CAPACITY = 16
DATALINK_QUEUE_CAPACITY = 16


def recommended_window_size(available_width: int, available_height: int) -> tuple[int, int]:
    width = min(1480, max(1180, int(available_width * 0.92)))
    height = min(960, max(820, int(available_height * 0.92)))
    return min(width, available_width), min(height, available_height)


def _status_value_label(parent: QtWidgets.QWidget) -> QtWidgets.QLabel:
    label = QtWidgets.QLabel("-", parent)
    label.setWordWrap(True)
    label.setTextInteractionFlags(QtCore.Qt.TextInteractionFlag.TextSelectableByMouse)
    label.setSizePolicy(
        QtWidgets.QSizePolicy.Policy.Expanding,
        QtWidgets.QSizePolicy.Policy.Preferred,
    )
    label.setObjectName("statusValue")
    return label


class IndicatorLamp(QtWidgets.QFrame):
    def __init__(self, parent: QtWidgets.QWidget | None = None, *, diameter: int = 18) -> None:
        super().__init__(parent)
        self._diameter = diameter
        self.setFixedSize(diameter, diameter)
        self.set_active(False)

    def set_active(self, active: bool) -> None:
        background = "#a8d37a" if active else "#d6dde8"
        border = "#2c4f87" if active else "#7f8fab"
        self.setStyleSheet(
            f"background:{background}; border:2px solid {border}; border-radius:{self._diameter // 2}px;"
        )


class EmblemWidget(QtWidgets.QWidget):
    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.setFixedSize(88, 88)

    def paintEvent(self, event: QtGui.QPaintEvent) -> None:  # noqa: ARG002
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing)
        rect = self.rect().adjusted(4, 4, -4, -4)

        outer_pen = QtGui.QPen(QtGui.QColor("#1c62b4"), 4)
        inner_pen = QtGui.QPen(QtGui.QColor("#1c62b4"), 2)
        painter.setPen(outer_pen)
        painter.setBrush(QtGui.QColor("#ffffff"))
        painter.drawEllipse(rect)

        inner_rect = rect.adjusted(10, 10, -10, -10)
        painter.setPen(inner_pen)
        painter.drawEllipse(inner_rect)

        center = inner_rect.center()
        painter.drawLine(
            QtCore.QPointF(center.x(), inner_rect.top() + 6),
            QtCore.QPointF(center.x(), inner_rect.bottom() - 6),
        )
        painter.drawLine(
            QtCore.QPointF(inner_rect.left() + 6, center.y()),
            QtCore.QPointF(inner_rect.right() - 6, center.y()),
        )
        painter.drawEllipse(center, 6, 6)

        painter.setPen(QtGui.QPen(QtGui.QColor("#1c62b4"), 2))
        painter.drawArc(inner_rect.adjusted(8, 14, -8, -14), 40 * 16, 100 * 16)
        painter.drawArc(inner_rect.adjusted(8, 14, -8, -14), 220 * 16, 100 * 16)

        painter.setPen(QtGui.QColor("#1c62b4"))
        year_font = QtGui.QFont("Songti SC", 8)
        painter.setFont(year_font)
        painter.drawText(rect.adjusted(0, 0, 0, -2), QtCore.Qt.AlignmentFlag.AlignBottom | QtCore.Qt.AlignmentFlag.AlignHCenter, "1960")


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, runtime: RuntimeService, settings: AppSettings) -> None:
        super().__init__()
        self._runtime = runtime
        self._settings = settings
        self._data_mode = "raw"
        self._status_labels: dict[str, list[QtWidgets.QLabel]] = {}
        self._status_lamps: dict[str, list[IndicatorLamp]] = {}
        self._plots: list[pg.PlotDataItem | None] = [None] * self._settings.protocol.channels
        self._queue_bars: dict[str, QtWidgets.QProgressBar] = {}
        self._queue_value_labels: dict[str, QtWidgets.QLabel] = {}
        self._mode_selectors: list[QtWidgets.QComboBox] = []
        self._plot_window_seconds_spin: QtWidgets.QDoubleSpinBox | None = None

        self._processing_state_label: QtWidgets.QLabel | None = None
        self._ingest_help_label: QtWidgets.QLabel | None = None
        self._config_feedback_label: QtWidgets.QLabel | None = None
        self._start_processing_button: QtWidgets.QPushButton | None = None
        self._pause_processing_button: QtWidgets.QPushButton | None = None
        self._apply_button: QtWidgets.QPushButton | None = None
        self._reload_button: QtWidgets.QPushButton | None = None

        self._data1_rate_spin: QtWidgets.QDoubleSpinBox | None = None
        self._data2_rate_spin: QtWidgets.QDoubleSpinBox | None = None
        self._data_server_mode_combo: QtWidgets.QComboBox | None = None
        self._data_host_edit: QtWidgets.QLineEdit | None = None
        self._data_port_spin: QtWidgets.QSpinBox | None = None
        self._data_remote_host_edit: QtWidgets.QLineEdit | None = None
        self._data_remote_port_spin: QtWidgets.QSpinBox | None = None
        self._connection_mode_hint_label: QtWidgets.QLabel | None = None

        self._frame_header_edit: QtWidgets.QLineEdit | None = None
        self._frame_header_size_combo: QtWidgets.QComboBox | None = None
        self._length_field_size_combo: QtWidgets.QComboBox | None = None
        self._length_field_format_combo: QtWidgets.QComboBox | None = None
        self._length_field_units_combo: QtWidgets.QComboBox | None = None
        self._byte_order_combo: QtWidgets.QComboBox | None = None
        self._channel_layout_combo: QtWidgets.QComboBox | None = None

        self._storage_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._storage_root_edit: QtWidgets.QLineEdit | None = None
        self._storage_browse_button: QtWidgets.QPushButton | None = None
        self._storage_duration_spin: QtWidgets.QSpinBox | None = None
        self._storage_output_data_type_combo: QtWidgets.QComboBox | None = None
        self._storage_int32_gain_spin: QtWidgets.QDoubleSpinBox | None = None
        self._storage_network_edit: QtWidgets.QLineEdit | None = None
        self._storage_station_edit: QtWidgets.QLineEdit | None = None
        self._storage_location_edit: QtWidgets.QLineEdit | None = None
        self._storage_channel_codes_table: QtWidgets.QTableWidget | None = None

        self._datalink_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._datalink_host_edit: QtWidgets.QLineEdit | None = None
        self._datalink_port_spin: QtWidgets.QSpinBox | None = None
        self._datalink_stream_template_edit: QtWidgets.QLineEdit | None = None
        self._datalink_ack_checkbox: QtWidgets.QCheckBox | None = None
        self._datalink_send_data2_checkbox: QtWidgets.QCheckBox | None = None

        self._capture_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._capture_path_edit: QtWidgets.QLineEdit | None = None
        self._capture_browse_button: QtWidgets.QPushButton | None = None

        self._gnss_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._gnss_port_combo: QtWidgets.QComboBox | None = None
        self._gnss_refresh_button: QtWidgets.QPushButton | None = None
        self._gnss_baudrate_spin: QtWidgets.QSpinBox | None = None
        self._gnss_mode_combo: QtWidgets.QComboBox | None = None
        self._gnss_poll_spin: QtWidgets.QDoubleSpinBox | None = None
        self._gnss_timestamp_interval_spin: QtWidgets.QDoubleSpinBox | None = None

        self._log_view: QtWidgets.QPlainTextEdit | None = None
        self._log_level_combo: QtWidgets.QComboBox | None = None

        self._gnss_last_timestamp_label: QtWidgets.QLabel | None = None
        self._gnss_last_error_label: QtWidgets.QLabel | None = None
        self._remote_web_label: QtWidgets.QLabel | None = None
        self._remote_control_label: QtWidgets.QLabel | None = None

        self.setWindowTitle("高精度长基线光纤应变信号解调软件")
        self.statusBar().showMessage("就绪")
        pg.setConfigOptions(antialias=True)
        self._build_ui()
        self._apply_styles()
        self._configure_window_geometry()
        self._load_runtime_config_into_form()
        self._refresh_gnss_ports()

        self._timer = QtCore.QTimer(self)
        self._timer.timeout.connect(self._refresh)
        self._timer.start(settings.gui.refresh_interval_ms)

    def _build_ui(self) -> None:
        root = QtWidgets.QWidget(self)
        outer = QtWidgets.QVBoxLayout(root)
        outer.setContentsMargins(22, 22, 22, 22)

        shell = QtWidgets.QFrame(root)
        shell.setObjectName("shell")
        shell_layout = QtWidgets.QVBoxLayout(shell)
        shell_layout.setContentsMargins(18, 18, 18, 18)
        shell_layout.setSpacing(16)
        shell_layout.addWidget(self._build_header_banner())

        tabs = QtWidgets.QTabWidget(shell)
        tabs.setDocumentMode(True)
        tabs.addTab(self._build_settings_tab(), "参数设置")
        tabs.addTab(self._build_waveform_tab(), "波形显示")
        tabs.addTab(self._build_status_tab(), "状态监控")
        tabs.addTab(self._build_remote_tab(), "远程传输")
        shell_layout.addWidget(tabs, stretch=1)

        outer.addWidget(shell)
        self.setCentralWidget(root)

    def _build_header_banner(self) -> QtWidgets.QWidget:
        banner = QtWidgets.QFrame(self)
        banner.setObjectName("banner")
        layout = QtWidgets.QHBoxLayout(banner)
        layout.setContentsMargins(20, 12, 20, 12)
        layout.setSpacing(18)

        layout.addSpacing(72)

        title = QtWidgets.QLabel("高精度长基线光纤应变信号解调软件", banner)
        title.setObjectName("bannerTitle")
        title.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title, stretch=1)

        layout.addWidget(EmblemWidget(banner), stretch=0, alignment=QtCore.Qt.AlignmentFlag.AlignRight)
        return banner

    def _build_settings_tab(self) -> QtWidgets.QWidget:
        scroll = QtWidgets.QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)

        container = QtWidgets.QWidget(scroll)
        layout = QtWidgets.QVBoxLayout(container)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(16)
        layout.addWidget(self._build_control_strip())

        content = QtWidgets.QHBoxLayout()
        content.setSpacing(16)
        content.addWidget(self._build_ingest_panel(), stretch=5)
        content.addWidget(self._build_storage_panel(), stretch=5)
        content.addWidget(self._build_gnss_panel(), stretch=4)
        layout.addLayout(content)
        layout.addStretch(1)

        scroll.setWidget(container)
        return scroll

    def _build_waveform_tab(self) -> QtWidgets.QWidget:
        widget = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(16)
        layout.addWidget(
            self._build_page_intro(
                "波形显示",
                "按参考布局展示 8 路通道波形，支持在原始、降采样1、降采样2之间切换。",
            )
        )

        workspace = QtWidgets.QFrame(widget)
        workspace.setObjectName("workspace")
        workspace_layout = QtWidgets.QVBoxLayout(workspace)
        workspace_layout.setContentsMargins(18, 16, 18, 16)
        workspace_layout.setSpacing(16)

        toolbar = QtWidgets.QHBoxLayout()
        toolbar.addWidget(QtWidgets.QLabel("显示数据", workspace))
        toolbar.addWidget(self._create_mode_selector(workspace))
        toolbar.addSpacing(18)
        toolbar.addWidget(QtWidgets.QLabel("显示时长", workspace))
        toolbar.addWidget(self._create_plot_window_seconds_spin(workspace))
        toolbar.addStretch(1)
        toolbar.addWidget(QtWidgets.QLabel("当前源采样率", workspace))
        toolbar.addWidget(self._create_snapshot_label("source_sample_rate", workspace))
        workspace_layout.addLayout(toolbar)

        grid = QtWidgets.QGridLayout()
        grid.setHorizontalSpacing(16)
        grid.setVerticalSpacing(16)
        mapping = [
            ("地表应变", 0, "钻孔#1", 2),
            ("深井应变", 1, "钻孔#2", 3),
            ("地表温度", 6, "钻孔#3", 4),
            ("深井温度", 7, "钻孔#4", 5),
        ]
        for row, (left_label, left_channel, center_label, right_channel) in enumerate(mapping):
            left_text = QtWidgets.QLabel(left_label, workspace)
            left_text.setObjectName("laneLabel")
            left_text.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            grid.addWidget(left_text, row, 0)

            grid.addWidget(self._build_channel_card(left_channel, workspace), row, 1)

            center_text = QtWidgets.QLabel(center_label, workspace)
            center_text.setObjectName("laneLabel")
            center_text.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            grid.addWidget(center_text, row, 2)

            grid.addWidget(self._build_channel_card(right_channel, workspace), row, 3)

        grid.setColumnStretch(1, 1)
        grid.setColumnStretch(3, 1)
        workspace_layout.addLayout(grid, stretch=1)
        layout.addWidget(workspace, stretch=1)
        return widget

    def _build_status_tab(self) -> QtWidgets.QWidget:
        widget = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(16)
        layout.addWidget(
            self._build_page_intro(
                "状态监控",
                "集中查看数据链路、处理队列、本地存储、远传发布和 GNSS 状态，顶部使用条形进度显示队列压力。",
            )
        )

        top = QtWidgets.QFrame(widget)
        top.setObjectName("workspace")
        top_layout = QtWidgets.QGridLayout(top)
        top_layout.setContentsMargins(18, 18, 18, 18)
        top_layout.setSpacing(16)
        top_layout.addWidget(
            self._build_queue_card(
                "实时处理队列",
                "processing",
                PROCESSING_QUEUE_CAPACITY,
                "data_connected",
                [
                    ("source_sample_rate", "源采样率"),
                    ("packets_received", "已收包数"),
                    ("frames_dropped", "处理丢帧"),
                ],
            ),
            0,
            0,
        )
        top_layout.addWidget(
            self._build_queue_card(
                "本地存储队列",
                "storage",
                STORAGE_QUEUE_CAPACITY,
                "storage_enabled",
                [
                    ("storage_disk_usage_percent", "磁盘利用率"),
                    ("storage_disk_free_bytes", "磁盘可用"),
                    ("storage_frames_dropped", "存储丢帧"),
                    ("storage_last_error", "存储错误"),
                ],
            ),
            0,
            1,
        )
        top_layout.addWidget(
            self._build_queue_card(
                "远传发布队列",
                "datalink",
                DATALINK_QUEUE_CAPACITY,
                "datalink_connected",
                [
                    ("datalink_packets_sent", "已发包数"),
                    ("datalink_reconnects", "远传重连"),
                    ("datalink_publish_last_error", "发布错误"),
                ],
            ),
            1,
            0,
        )
        top_layout.addWidget(
            self._build_summary_card(
                "运行摘要",
                [
                    ("control_connected", "控制连接"),
                    ("gnss_connected", "GNSS 连接"),
                    ("bytes_received", "已收字节"),
                    ("datalink_bytes_sent", "已发字节"),
                    ("last_error", "最近错误"),
                ],
            ),
            1,
            1,
        )
        top_layout.setColumnStretch(0, 1)
        top_layout.setColumnStretch(1, 1)
        layout.addWidget(top, stretch=3)

        log_card = self._panel_card("运行日志", widget)
        log_layout = QtWidgets.QVBoxLayout(log_card)
        controls = QtWidgets.QHBoxLayout()
        controls.addWidget(QtWidgets.QLabel("筛选级别", log_card))
        self._log_level_combo = QtWidgets.QComboBox(log_card)
        self._log_level_combo.addItems(["全部", "信息", "警告", "错误"])
        controls.addWidget(self._log_level_combo)
        controls.addStretch(1)
        log_layout.addLayout(controls)

        self._log_view = QtWidgets.QPlainTextEdit(log_card)
        self._log_view.setReadOnly(True)
        log_layout.addWidget(self._log_view, stretch=1)
        layout.addWidget(log_card, stretch=2)
        return widget

    def _build_remote_tab(self) -> QtWidgets.QWidget:
        widget = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(16)
        layout.addWidget(
            self._build_page_intro(
                "远程传输",
                "把 DataLink 远传和远程网页/控制端相关信息集中到这一页，便于部署现场统一查看。",
            )
        )

        workspace = QtWidgets.QFrame(widget)
        workspace.setObjectName("workspace")
        workspace_layout = QtWidgets.QHBoxLayout(workspace)
        workspace_layout.setContentsMargins(18, 18, 18, 18)
        workspace_layout.setSpacing(18)

        datalink_card = self._panel_card("DataLink 远传配置", workspace)
        datalink_layout = QtWidgets.QVBoxLayout(datalink_card)
        datalink_header = QtWidgets.QHBoxLayout()
        datalink_header.addWidget(QtWidgets.QLabel("远传状态", datalink_card))
        lamp = IndicatorLamp(datalink_card)
        self._register_lamp("datalink_connected", lamp)
        datalink_header.addWidget(lamp)
        datalink_header.addStretch(1)
        datalink_header.addWidget(self._create_snapshot_label("datalink_connected", datalink_card))
        datalink_layout.addLayout(datalink_header)

        form = QtWidgets.QFormLayout()
        self._datalink_enabled_checkbox = QtWidgets.QCheckBox("启用 DataLink 远传", datalink_card)
        self._datalink_enabled_checkbox.toggled.connect(self._update_form_state)
        self._datalink_host_edit = QtWidgets.QLineEdit(datalink_card)
        self._datalink_port_spin = QtWidgets.QSpinBox(datalink_card)
        self._datalink_port_spin.setRange(1, 65535)
        self._datalink_stream_template_edit = QtWidgets.QLineEdit(datalink_card)
        self._datalink_ack_checkbox = QtWidgets.QCheckBox("发送后等待 ACK", datalink_card)
        self._datalink_send_data2_checkbox = QtWidgets.QCheckBox("同时发送降采样2", datalink_card)

        datalink_layout.addWidget(self._datalink_enabled_checkbox)
        form.addRow("远传主机", self._datalink_host_edit)
        form.addRow("远传端口", self._datalink_port_spin)
        form.addRow("流模板", self._datalink_stream_template_edit)
        form.addRow("确认策略", self._datalink_ack_checkbox)
        form.addRow("发送内容", self._datalink_send_data2_checkbox)
        datalink_layout.addLayout(form)

        tip = QtWidgets.QLabel(
            "流模板可使用 {network}、{station}、{location}、{channel}、{group} 占位符。",
            datalink_card,
        )
        tip.setWordWrap(True)
        tip.setObjectName("mutedText")
        datalink_layout.addWidget(tip)
        datalink_layout.addStretch(1)
        workspace_layout.addWidget(datalink_card, stretch=3)

        side_column = QtWidgets.QVBoxLayout()
        side_column.setSpacing(16)
        side_column.addWidget(
            self._build_summary_card(
                "远传运行状态",
                [
                    ("datalink_enabled", "远传开关"),
                    ("datalink_packets_sent", "已发包数"),
                    ("datalink_bytes_sent", "已发字节"),
                    ("datalink_publish_queue_depth", "发布队列"),
                    ("datalink_last_error", "连接错误"),
                    ("datalink_publish_last_error", "发布错误"),
                ],
            )
        )

        access_card = self._panel_card("远程接入提示", workspace)
        access_layout = QtWidgets.QFormLayout(access_card)
        self._remote_web_label = _status_value_label(access_card)
        self._remote_control_label = _status_value_label(access_card)
        access_layout.addRow("Web API", self._remote_web_label)
        access_layout.addRow("控制端口", self._remote_control_label)
        access_layout.addRow("控制连接", self._create_snapshot_label("control_connected", access_card))
        access_layout.addRow("GNSS 时间", self._create_snapshot_label("gnss_last_timestamp", access_card))
        side_column.addWidget(access_card)
        side_column.addStretch(1)

        workspace_layout.addLayout(side_column, stretch=2)
        layout.addWidget(workspace, stretch=1)
        return widget

    def _build_page_intro(self, title: str, description: str) -> QtWidgets.QWidget:
        frame = QtWidgets.QFrame(self)
        frame.setObjectName("introStrip")
        layout = QtWidgets.QVBoxLayout(frame)
        layout.setContentsMargins(14, 10, 14, 10)
        title_label = QtWidgets.QLabel(title, frame)
        title_label.setObjectName("pageTitle")
        desc_label = QtWidgets.QLabel(description, frame)
        desc_label.setWordWrap(True)
        desc_label.setObjectName("mutedText")
        layout.addWidget(title_label)
        layout.addWidget(desc_label)
        return frame

    def _build_control_strip(self) -> QtWidgets.QWidget:
        frame = QtWidgets.QFrame(self)
        frame.setObjectName("controlStrip")
        layout = QtWidgets.QVBoxLayout(frame)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(10)

        top = QtWidgets.QHBoxLayout()
        self._processing_state_label = QtWidgets.QLabel(frame)
        self._processing_state_label.setObjectName("stateText")
        self._reload_button = QtWidgets.QPushButton("重载配置到界面", frame)
        self._reload_button.clicked.connect(self._load_runtime_config_into_form)
        self._apply_button = QtWidgets.QPushButton("应用配置", frame)
        self._apply_button.clicked.connect(self._apply_runtime_config)
        self._start_processing_button = QtWidgets.QPushButton("启动数据接收", frame)
        self._start_processing_button.clicked.connect(self._start_processing)
        self._pause_processing_button = QtWidgets.QPushButton("停止数据接收", frame)
        self._pause_processing_button.clicked.connect(self._pause_processing)
        top.addWidget(self._processing_state_label)
        top.addStretch(1)
        top.addWidget(self._reload_button)
        top.addWidget(self._apply_button)
        top.addWidget(self._start_processing_button)
        top.addWidget(self._pause_processing_button)

        self._ingest_help_label = QtWidgets.QLabel(frame)
        self._ingest_help_label.setWordWrap(True)
        self._ingest_help_label.setObjectName("mutedText")
        self._config_feedback_label = QtWidgets.QLabel("表单已同步到当前运行时配置。", frame)
        self._config_feedback_label.setWordWrap(True)
        self._config_feedback_label.setObjectName("feedbackText")

        layout.addLayout(top)
        layout.addWidget(self._ingest_help_label)
        layout.addWidget(self._config_feedback_label)
        self._update_processing_controls()
        return frame

    def _build_ingest_panel(self) -> QtWidgets.QWidget:
        card = self._panel_card("数据接收", self)
        layout = QtWidgets.QVBoxLayout(card)
        layout.setSpacing(12)

        header = QtWidgets.QHBoxLayout()
        header.addWidget(QtWidgets.QLabel("链路状态", card))
        lamp = IndicatorLamp(card)
        self._register_lamp("data_connected", lamp)
        header.addWidget(lamp)
        header.addStretch(1)
        header.addWidget(self._create_snapshot_label("data_connected", card))
        layout.addLayout(header)

        form = QtWidgets.QFormLayout()
        self._data_server_mode_combo = QtWidgets.QComboBox(card)
        self._data_server_mode_combo.addItem("主动连接设备", "client")
        self._data_server_mode_combo.addItem("监听设备连接", "server")
        self._data_server_mode_combo.currentIndexChanged.connect(self._update_form_state)

        self._data_host_edit = QtWidgets.QLineEdit(card)
        self._data_port_spin = QtWidgets.QSpinBox(card)
        self._data_port_spin.setRange(1, 65535)
        self._data_remote_host_edit = QtWidgets.QLineEdit(card)
        self._data_remote_port_spin = QtWidgets.QSpinBox(card)
        self._data_remote_port_spin.setRange(1, 65535)

        self._frame_header_edit = QtWidgets.QLineEdit(card)
        self._frame_header_size_combo = QtWidgets.QComboBox(card)
        self._frame_header_size_combo.addItems(["2", "4", "8"])
        self._length_field_size_combo = QtWidgets.QComboBox(card)
        self._length_field_size_combo.addItems(["4", "8"])
        self._length_field_format_combo = QtWidgets.QComboBox(card)
        self._length_field_format_combo.addItem("无符号整数", "uint")
        self._length_field_format_combo.addItem("浮点 float64", "float64")
        self._length_field_units_combo = QtWidgets.QComboBox(card)
        self._length_field_units_combo.addItem("字节", "bytes")
        self._length_field_units_combo.addItem("数值个数", "values")
        self._byte_order_combo = QtWidgets.QComboBox(card)
        self._byte_order_combo.addItem("大端", "big")
        self._byte_order_combo.addItem("小端", "little")
        self._channel_layout_combo = QtWidgets.QComboBox(card)
        self._channel_layout_combo.addItem("采样交织", "interleaved")
        self._channel_layout_combo.addItem("按通道连续", "channel-major")

        self._data1_rate_spin = QtWidgets.QDoubleSpinBox(card)
        self._data1_rate_spin.setRange(0.1, 10000.0)
        self._data1_rate_spin.setDecimals(2)
        self._data2_rate_spin = QtWidgets.QDoubleSpinBox(card)
        self._data2_rate_spin.setRange(0.1, 10000.0)
        self._data2_rate_spin.setDecimals(2)

        form.addRow("接入模式", self._data_server_mode_combo)
        form.addRow("本地监听地址", self._data_host_edit)
        form.addRow("本地监听端口", self._data_port_spin)
        form.addRow("设备地址", self._data_remote_host_edit)
        form.addRow("设备端口", self._data_remote_port_spin)
        form.addRow("帧头值", self._frame_header_edit)
        form.addRow("帧头字节数", self._frame_header_size_combo)
        form.addRow("长度字段字节数", self._length_field_size_combo)
        form.addRow("长度字段格式", self._length_field_format_combo)
        form.addRow("长度单位", self._length_field_units_combo)
        form.addRow("字节序", self._byte_order_combo)
        form.addRow("通道排列", self._channel_layout_combo)
        form.addRow("采样率1", self._data1_rate_spin)
        form.addRow("采样率2", self._data2_rate_spin)
        layout.addLayout(form)

        self._connection_mode_hint_label = QtWidgets.QLabel(card)
        self._connection_mode_hint_label.setWordWrap(True)
        self._connection_mode_hint_label.setObjectName("mutedText")
        layout.addWidget(self._connection_mode_hint_label)
        layout.addStretch(1)
        return card

    def _build_storage_panel(self) -> QtWidgets.QWidget:
        card = self._panel_card("文件存储", self)
        layout = QtWidgets.QVBoxLayout(card)
        layout.setSpacing(12)

        header = QtWidgets.QHBoxLayout()
        header.addWidget(QtWidgets.QLabel("存储状态", card))
        lamp = IndicatorLamp(card)
        self._register_lamp("storage_enabled", lamp)
        header.addWidget(lamp)
        header.addStretch(1)
        header.addWidget(self._create_snapshot_label("storage_enabled", card))
        layout.addLayout(header)

        self._storage_enabled_checkbox = QtWidgets.QCheckBox("启用文件存储", card)
        self._storage_enabled_checkbox.toggled.connect(self._update_form_state)
        layout.addWidget(self._storage_enabled_checkbox)

        form = QtWidgets.QFormLayout()
        self._storage_root_edit = QtWidgets.QLineEdit(card)
        self._storage_browse_button = QtWidgets.QPushButton("浏览...", card)
        self._storage_browse_button.clicked.connect(self._choose_storage_root)
        storage_row = QtWidgets.QHBoxLayout()
        storage_row.addWidget(self._storage_root_edit, stretch=1)
        storage_row.addWidget(self._storage_browse_button)
        storage_widget = QtWidgets.QWidget(card)
        storage_widget.setLayout(storage_row)

        self._storage_duration_spin = QtWidgets.QSpinBox(card)
        self._storage_duration_spin.setRange(1, 86400)
        self._storage_output_data_type_combo = QtWidgets.QComboBox(card)
        self._storage_output_data_type_combo.addItem("float32", "float32")
        self._storage_output_data_type_combo.addItem("INT32", "int32")
        self._storage_output_data_type_combo.currentIndexChanged.connect(self._update_form_state)
        self._storage_int32_gain_spin = QtWidgets.QDoubleSpinBox(card)
        self._storage_int32_gain_spin.setRange(0.000001, 1_000_000_000_000.0)
        self._storage_int32_gain_spin.setDecimals(6)
        self._storage_int32_gain_spin.setSingleStep(1000.0)
        self._storage_network_edit = QtWidgets.QLineEdit(card)
        self._storage_station_edit = QtWidgets.QLineEdit(card)
        self._storage_location_edit = QtWidgets.QLineEdit(card)
        self._storage_channel_codes_table = self._create_channel_codes_table(card)

        self._capture_enabled_checkbox = QtWidgets.QCheckBox("启用原始 TCP 抓包", card)
        self._capture_enabled_checkbox.toggled.connect(self._update_form_state)
        self._capture_path_edit = QtWidgets.QLineEdit(card)
        self._capture_browse_button = QtWidgets.QPushButton("抓包文件...", card)
        self._capture_browse_button.clicked.connect(self._choose_capture_path)
        capture_row = QtWidgets.QHBoxLayout()
        capture_row.addWidget(self._capture_path_edit, stretch=1)
        capture_row.addWidget(self._capture_browse_button)
        capture_widget = QtWidgets.QWidget(card)
        capture_widget.setLayout(capture_row)

        form.addRow("存储目录", storage_widget)
        form.addRow("单文件时长(秒)", self._storage_duration_spin)
        form.addRow("数据类型(存储/远传)", self._storage_output_data_type_combo)
        form.addRow("增益", self._storage_int32_gain_spin)
        form.addRow("网络码", self._storage_network_edit)
        form.addRow("台站码", self._storage_station_edit)
        form.addRow("位置码", self._storage_location_edit)
        form.addRow("通道码", self._storage_channel_codes_table)
        form.addRow(self._capture_enabled_checkbox)
        form.addRow("抓包文件", capture_widget)
        layout.addLayout(form)

        runtime_form = QtWidgets.QFormLayout()
        runtime_form.addRow("存储队列", self._create_snapshot_label("storage_queue_depth", card))
        runtime_form.addRow("存储丢帧", self._create_snapshot_label("storage_frames_dropped", card))
        runtime_form.addRow("磁盘总容量", self._create_snapshot_label("storage_disk_total_bytes", card))
        runtime_form.addRow("磁盘利用率", self._create_snapshot_label("storage_disk_usage_percent", card))
        runtime_form.addRow("磁盘可用", self._create_snapshot_label("storage_disk_free_bytes", card))
        runtime_form.addRow("最近错误", self._create_snapshot_label("storage_last_error", card))
        layout.addLayout(runtime_form)
        layout.addStretch(1)
        return card

    def _build_gnss_panel(self) -> QtWidgets.QWidget:
        card = self._panel_card("GNSS 设置", self)
        layout = QtWidgets.QVBoxLayout(card)
        layout.setSpacing(12)

        header = QtWidgets.QHBoxLayout()
        header.addWidget(QtWidgets.QLabel("GNSS 状态", card))
        lamp = IndicatorLamp(card)
        self._register_lamp("gnss_connected", lamp)
        header.addWidget(lamp)
        header.addStretch(1)
        header.addWidget(self._create_snapshot_label("gnss_connected", card))
        layout.addLayout(header)

        self._gnss_enabled_checkbox = QtWidgets.QCheckBox("启用 GNSS 时间", card)
        self._gnss_enabled_checkbox.toggled.connect(self._update_form_state)
        layout.addWidget(self._gnss_enabled_checkbox)

        form = QtWidgets.QFormLayout()
        self._gnss_port_combo = QtWidgets.QComboBox(card)
        self._gnss_port_combo.setEditable(True)
        self._gnss_port_combo.setInsertPolicy(QtWidgets.QComboBox.InsertPolicy.NoInsert)
        self._gnss_refresh_button = QtWidgets.QPushButton("刷新串口", card)
        self._gnss_refresh_button.clicked.connect(self._refresh_gnss_ports)
        port_row = QtWidgets.QHBoxLayout()
        port_row.addWidget(self._gnss_port_combo, stretch=1)
        port_row.addWidget(self._gnss_refresh_button)
        port_widget = QtWidgets.QWidget(card)
        port_widget.setLayout(port_row)

        self._gnss_baudrate_spin = QtWidgets.QSpinBox(card)
        self._gnss_baudrate_spin.setRange(1, 921600)
        self._gnss_mode_combo = QtWidgets.QComboBox(card)
        self._gnss_mode_combo.addItem("调试模式", "debug")
        self._gnss_mode_combo.addItem("部署模式", "deploy")
        self._gnss_poll_spin = QtWidgets.QDoubleSpinBox(card)
        self._gnss_poll_spin.setRange(0.01, 10.0)
        self._gnss_poll_spin.setDecimals(2)
        self._gnss_timestamp_interval_spin = QtWidgets.QDoubleSpinBox(card)
        self._gnss_timestamp_interval_spin.setRange(0.001, 10.0)
        self._gnss_timestamp_interval_spin.setDecimals(3)

        form.addRow("串口", port_widget)
        form.addRow("波特率", self._gnss_baudrate_spin)
        form.addRow("模式", self._gnss_mode_combo)
        form.addRow("轮询间隔(秒)", self._gnss_poll_spin)
        form.addRow("授时等待超时(秒)", self._gnss_timestamp_interval_spin)
        layout.addLayout(form)

        runtime_form = QtWidgets.QFormLayout()
        self._gnss_last_timestamp_label = _status_value_label(card)
        self._gnss_last_error_label = _status_value_label(card)
        runtime_form.addRow("UTC 时间", self._gnss_last_timestamp_label)
        runtime_form.addRow("最近错误", self._gnss_last_error_label)
        runtime_form.addRow("回退模式", self._create_snapshot_label("gnss_fallback_active", card))
        layout.addLayout(runtime_form)
        layout.addStretch(1)
        return card

    def _build_channel_card(self, channel_index: int, parent: QtWidgets.QWidget) -> QtWidgets.QWidget:
        card = QtWidgets.QFrame(parent)
        card.setObjectName("plotCard")
        layout = QtWidgets.QVBoxLayout(card)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        title = QtWidgets.QLabel(f"CH{channel_index + 1}", card)
        title.setObjectName("plotCardTitle")
        title.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        plot = self._create_plot_widget(parent=card)
        plot.setMinimumHeight(150)
        curve = plot.plot(pen=pg.mkPen(self._channel_color(channel_index), width=1.8))
        self._plots[channel_index] = curve
        layout.addWidget(plot, stretch=1)
        return card

    def _build_queue_card(
        self,
        title: str,
        key: str,
        capacity: int,
        indicator_key: str,
        fields: list[tuple[str, str]],
    ) -> QtWidgets.QWidget:
        card = self._panel_card(title, self)
        layout = QtWidgets.QVBoxLayout(card)
        layout.setSpacing(10)

        header = QtWidgets.QHBoxLayout()
        header.addWidget(QtWidgets.QLabel("运行状态", card))
        lamp = IndicatorLamp(card)
        self._register_lamp(indicator_key, lamp)
        header.addWidget(lamp)
        header.addStretch(1)
        layout.addLayout(header)

        bar = QtWidgets.QProgressBar(card)
        bar.setRange(0, capacity)
        bar.setValue(0)
        bar.setTextVisible(False)
        self._queue_bars[key] = bar
        layout.addWidget(bar)

        value_label = QtWidgets.QLabel(f"0 / {capacity}", card)
        value_label.setObjectName("queueValue")
        self._queue_value_labels[key] = value_label
        layout.addWidget(value_label)

        form = QtWidgets.QFormLayout()
        for field_key, label_text in fields:
            form.addRow(label_text, self._create_snapshot_label(field_key, card))
        layout.addLayout(form)
        return card

    def _build_summary_card(self, title: str, fields: list[tuple[str, str]]) -> QtWidgets.QWidget:
        card = self._panel_card(title, self)
        layout = QtWidgets.QFormLayout(card)
        for key, label_text in fields:
            layout.addRow(label_text, self._create_snapshot_label(key, card))
        return card

    def _create_channel_codes_table(self, parent: QtWidgets.QWidget) -> QtWidgets.QTableWidget:
        table = QtWidgets.QTableWidget(self._settings.protocol.channels, 2, parent)
        table.setHorizontalHeaderLabels(["通道", "通道码"])
        table.verticalHeader().setVisible(False)
        table.setAlternatingRowColors(True)
        table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.NoSelection)
        table.setEditTriggers(
            QtWidgets.QAbstractItemView.EditTrigger.DoubleClicked
            | QtWidgets.QAbstractItemView.EditTrigger.EditKeyPressed
            | QtWidgets.QAbstractItemView.EditTrigger.AnyKeyPressed
        )
        for row in range(self._settings.protocol.channels):
            channel_item = QtWidgets.QTableWidgetItem(f"CH{row + 1}")
            channel_item.setFlags(channel_item.flags() & ~QtCore.Qt.ItemFlag.ItemIsEditable)
            table.setItem(row, 0, channel_item)
            table.setItem(row, 1, QtWidgets.QTableWidgetItem(""))
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QtWidgets.QHeaderView.ResizeMode.Stretch)
        table.setMinimumHeight(min(320, 34 * (self._settings.protocol.channels + 1)))
        table.setMaximumHeight(360)
        return table

    def _create_plot_widget(self, *, parent: QtWidgets.QWidget | None = None) -> pg.PlotWidget:
        plot = pg.PlotWidget(parent=parent)
        plot.setBackground("w")
        plot.showGrid(x=True, y=True, alpha=0.18)
        plot.setMenuEnabled(False)
        plot.setMouseEnabled(x=False, y=False)
        plot.hideButtons()
        axis_pen = pg.mkPen("#53657f", width=1)
        text_pen = pg.mkPen("#1d2d44", width=1)
        for axis_name in ("left", "bottom"):
            axis = plot.getAxis(axis_name)
            axis.setPen(axis_pen)
            axis.setTextPen(text_pen.color())
        plot.setLabel("bottom", "时间", units="s")
        return plot

    def _create_mode_selector(self, parent: QtWidgets.QWidget) -> QtWidgets.QComboBox:
        combo = QtWidgets.QComboBox(parent)
        combo.addItem("原始数据", "raw")
        combo.addItem("降采样1", "data1")
        combo.addItem("降采样2", "data2")
        combo.setCurrentIndex(0)
        combo.currentIndexChanged.connect(partial(self._on_mode_selector_changed, combo))
        self._mode_selectors.append(combo)
        return combo

    def _create_plot_window_seconds_spin(self, parent: QtWidgets.QWidget) -> QtWidgets.QDoubleSpinBox:
        spin = QtWidgets.QDoubleSpinBox(parent)
        spin.setRange(1.0, self._settings.gui.plot_history_seconds)
        spin.setDecimals(0)
        spin.setSingleStep(5.0)
        spin.setSuffix(" s")
        spin.setValue(self._settings.gui.plot_window_seconds)
        spin.valueChanged.connect(
            lambda value: setattr(self._settings.gui, "plot_window_seconds", float(value))
        )
        self._plot_window_seconds_spin = spin
        return spin

    def _on_mode_selector_changed(self, combo: QtWidgets.QComboBox, index: int) -> None:  # noqa: ARG002
        self._set_mode(str(combo.currentData()))

    def _create_snapshot_label(self, key: str, parent: QtWidgets.QWidget) -> QtWidgets.QLabel:
        label = _status_value_label(parent)
        self._status_labels.setdefault(key, []).append(label)
        return label

    def _register_lamp(self, key: str, lamp: IndicatorLamp) -> None:
        self._status_lamps.setdefault(key, []).append(lamp)

    def _panel_card(self, title: str, parent: QtWidgets.QWidget) -> QtWidgets.QGroupBox:
        group = QtWidgets.QGroupBox(title, parent)
        group.setObjectName("panelCard")
        return group

    def _configure_window_geometry(self) -> None:
        screen = QtWidgets.QApplication.primaryScreen()
        if screen is None:
            self.resize(1320, 900)
            return
        available = screen.availableGeometry()
        width, height = recommended_window_size(available.width(), available.height())
        self.resize(width, height)
        frame = self.frameGeometry()
        frame.moveCenter(available.center())
        self.move(frame.topLeft())

    def _load_runtime_config_into_form(self) -> None:
        config = self._runtime.current_config()

        assert self._data1_rate_spin is not None
        assert self._data2_rate_spin is not None
        assert self._data_server_mode_combo is not None
        assert self._data_host_edit is not None
        assert self._data_port_spin is not None
        assert self._data_remote_host_edit is not None
        assert self._data_remote_port_spin is not None
        assert self._frame_header_edit is not None
        assert self._frame_header_size_combo is not None
        assert self._length_field_size_combo is not None
        assert self._length_field_format_combo is not None
        assert self._length_field_units_combo is not None
        assert self._byte_order_combo is not None
        assert self._channel_layout_combo is not None
        assert self._storage_enabled_checkbox is not None
        assert self._storage_root_edit is not None
        assert self._storage_duration_spin is not None
        assert self._storage_output_data_type_combo is not None
        assert self._storage_int32_gain_spin is not None
        assert self._storage_network_edit is not None
        assert self._storage_station_edit is not None
        assert self._storage_location_edit is not None
        assert self._storage_channel_codes_table is not None
        assert self._datalink_enabled_checkbox is not None
        assert self._datalink_host_edit is not None
        assert self._datalink_port_spin is not None
        assert self._datalink_stream_template_edit is not None
        assert self._datalink_ack_checkbox is not None
        assert self._datalink_send_data2_checkbox is not None
        assert self._capture_enabled_checkbox is not None
        assert self._capture_path_edit is not None
        assert self._gnss_enabled_checkbox is not None
        assert self._gnss_port_combo is not None
        assert self._gnss_baudrate_spin is not None
        assert self._gnss_mode_combo is not None
        assert self._gnss_poll_spin is not None
        assert self._gnss_timestamp_interval_spin is not None

        processing = config["processing"]
        data_server = config["data_server"]
        protocol = config["protocol"]
        storage = config["storage"]
        datalink = config["datalink"]
        capture = config["capture"]
        gnss = config["gnss"]

        self._data1_rate_spin.setValue(processing["data1_rate"])
        self._data2_rate_spin.setValue(processing["data2_rate"])

        self._data_server_mode_combo.setCurrentIndex(0 if data_server["mode"] == "client" else 1)
        self._data_host_edit.setText(data_server["host"])
        self._data_port_spin.setValue(data_server["port"])
        self._data_remote_host_edit.setText(data_server["remote_host"])
        self._data_remote_port_spin.setValue(data_server["remote_port"])

        self._frame_header_edit.setText(str(protocol["frame_header"]))
        self._frame_header_size_combo.setCurrentText(str(protocol["frame_header_size"]))
        self._length_field_size_combo.setCurrentText(str(protocol["length_field_size"]))
        self._length_field_format_combo.setCurrentIndex(
            0 if protocol["length_field_format"] == "uint" else 1
        )
        self._length_field_units_combo.setCurrentIndex(0 if protocol["length_field_units"] == "bytes" else 1)
        self._byte_order_combo.setCurrentIndex(0 if protocol["byte_order"] == "big" else 1)
        self._channel_layout_combo.setCurrentIndex(
            0 if protocol["channel_layout"] == "interleaved" else 1
        )

        self._storage_enabled_checkbox.setChecked(storage["enabled"])
        self._storage_root_edit.setText(storage["root"])
        self._storage_duration_spin.setValue(storage["file_duration_seconds"])
        output_data_type_index = self._storage_output_data_type_combo.findData(
            str(storage.get("output_data_type", "float32")).lower()
        )
        self._storage_output_data_type_combo.setCurrentIndex(max(output_data_type_index, 0))
        self._storage_int32_gain_spin.setValue(float(storage.get("int32_gain", 1_000_000.0)))
        self._storage_network_edit.setText(storage["network"])
        self._storage_station_edit.setText(storage["station"])
        self._storage_location_edit.setText(storage["location"])
        self._set_channel_codes(storage.get("channel_codes", []))

        self._datalink_enabled_checkbox.setChecked(datalink["enabled"])
        self._datalink_host_edit.setText(datalink["host"])
        self._datalink_port_spin.setValue(datalink["port"])
        self._datalink_stream_template_edit.setText(datalink["stream_id_template"])
        self._datalink_ack_checkbox.setChecked(datalink["ack_required"])
        self._datalink_send_data2_checkbox.setChecked(datalink["send_data2"])

        self._capture_enabled_checkbox.setChecked(capture["enabled"])
        self._capture_path_edit.setText(capture["path"])

        self._gnss_enabled_checkbox.setChecked(gnss["enabled"])
        self._gnss_baudrate_spin.setValue(gnss["baudrate"])
        self._gnss_mode_combo.setCurrentIndex(0 if gnss["mode"] == "debug" else 1)
        self._gnss_poll_spin.setValue(gnss["poll_interval_seconds"])
        self._gnss_timestamp_interval_spin.setValue(
            gnss.get("packet_timestamp_timeout_seconds", gnss.get("timestamp_interval_seconds", 1.0))
        )
        self._refresh_gnss_ports(selected=gnss["port"])
        self._update_form_state()
        self._sync_mode_selectors()
        self._set_feedback("表单已同步到当前运行时配置。")

    def _set_mode(self, name: str, checked: bool = True) -> None:
        if not checked:
            return
        if name == self._data_mode:
            return
        self._data_mode = name
        self._sync_mode_selectors()

    def _sync_mode_selectors(self) -> None:
        for combo in self._mode_selectors:
            target_index = combo.findData(self._data_mode)
            if target_index < 0 or combo.currentIndex() == target_index:
                continue
            combo.blockSignals(True)
            combo.setCurrentIndex(target_index)
            combo.blockSignals(False)

    def _choose_storage_root(self) -> None:
        assert self._storage_root_edit is not None
        directory = QtWidgets.QFileDialog.getExistingDirectory(
            self,
            "选择存储目录",
            self._storage_root_edit.text(),
        )
        if directory:
            self._storage_root_edit.setText(directory)

    def _choose_capture_path(self) -> None:
        assert self._capture_path_edit is not None
        filename, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "选择抓包文件",
            self._capture_path_edit.text(),
            "抓包文件 (*.dlhcap);;所有文件 (*)",
        )
        if filename:
            self._capture_path_edit.setText(filename)

    def _refresh_gnss_ports(self, selected: str | None = None) -> None:
        if self._gnss_port_combo is None:
            return
        current = (selected if selected is not None else self._gnss_port_combo.currentText()).strip()
        ports = self._runtime.gnss_ports()
        self._gnss_port_combo.blockSignals(True)
        self._gnss_port_combo.clear()
        for port in ports:
            self._gnss_port_combo.addItem(port)
        if current:
            if current not in ports:
                self._gnss_port_combo.addItem(current)
            self._gnss_port_combo.setCurrentText(current)
        self._gnss_port_combo.blockSignals(False)

    def _update_form_state(self) -> None:
        if self._data_server_mode_combo is None:
            return
        client_mode = self._data_server_mode_combo.currentData() == "client"
        server_mode = not client_mode

        assert self._data_host_edit is not None
        assert self._data_port_spin is not None
        assert self._data_remote_host_edit is not None
        assert self._data_remote_port_spin is not None
        assert self._connection_mode_hint_label is not None
        assert self._datalink_stream_template_edit is not None
        assert self._storage_output_data_type_combo is not None
        assert self._storage_int32_gain_spin is not None

        self._data_host_edit.setEnabled(server_mode)
        self._data_port_spin.setEnabled(server_mode)
        self._data_remote_host_edit.setEnabled(client_mode)
        self._data_remote_port_spin.setEnabled(client_mode)
        if client_mode:
            self._connection_mode_hint_label.setText(
                "当前为主动连接设备模式：系统会主动连接“设备地址/设备端口”；本地监听地址与端口仅作为保留配置。"
            )
        else:
            self._connection_mode_hint_label.setText(
                "当前为监听设备连接模式：系统会在“本地监听地址/端口”等待设备主动接入。"
            )

        self._set_section_enabled(
            True,
            [
                self._storage_root_edit,
                self._storage_browse_button,
                self._storage_duration_spin,
                self._storage_output_data_type_combo,
                self._storage_int32_gain_spin,
                self._storage_network_edit,
                self._storage_station_edit,
                self._storage_location_edit,
                self._storage_channel_codes_table,
            ],
        )
        self._storage_int32_gain_spin.setEnabled(
            self._storage_output_data_type_combo.currentData() == "int32"
        )
        self._set_section_enabled(
            True,
            [
                self._datalink_host_edit,
                self._datalink_port_spin,
                self._datalink_stream_template_edit,
                self._datalink_ack_checkbox,
                self._datalink_send_data2_checkbox,
            ],
        )
        self._set_section_enabled(
            self._capture_enabled_checkbox.isChecked() if self._capture_enabled_checkbox is not None else False,
            [
                self._capture_path_edit,
                self._capture_browse_button,
            ],
        )
        self._set_section_enabled(
            self._gnss_enabled_checkbox.isChecked() if self._gnss_enabled_checkbox is not None else False,
            [
                self._gnss_port_combo,
                self._gnss_refresh_button,
                self._gnss_baudrate_spin,
                self._gnss_mode_combo,
                self._gnss_poll_spin,
                self._gnss_timestamp_interval_spin,
            ],
        )
        self._update_processing_controls()

    @staticmethod
    def _set_section_enabled(enabled: bool, widgets: list[QtWidgets.QWidget | None]) -> None:
        for widget in widgets:
            if widget is not None:
                widget.setEnabled(enabled)

    def _set_channel_codes(self, codes: list[str] | tuple[str, ...]) -> None:
        assert self._storage_channel_codes_table is not None
        for row in range(self._settings.protocol.channels):
            item = self._storage_channel_codes_table.item(row, 1)
            if item is None:
                item = QtWidgets.QTableWidgetItem("")
                self._storage_channel_codes_table.setItem(row, 1, item)
            item.setText(str(codes[row]) if row < len(codes) else "")

    def _channel_codes_from_table(self) -> list[str]:
        assert self._storage_channel_codes_table is not None
        codes: list[str] = []
        for row in range(self._settings.protocol.channels):
            item = self._storage_channel_codes_table.item(row, 1)
            code = "" if item is None else item.text().strip()
            if not code:
                raise ValueError(f"CH{row + 1} 通道码不能为空")
            codes.append(code)
        return codes

    def _apply_runtime_config(self) -> None:
        assert self._data1_rate_spin is not None
        assert self._data2_rate_spin is not None
        assert self._data_server_mode_combo is not None
        assert self._data_host_edit is not None
        assert self._data_port_spin is not None
        assert self._data_remote_host_edit is not None
        assert self._data_remote_port_spin is not None
        assert self._frame_header_edit is not None
        assert self._frame_header_size_combo is not None
        assert self._length_field_size_combo is not None
        assert self._length_field_format_combo is not None
        assert self._length_field_units_combo is not None
        assert self._byte_order_combo is not None
        assert self._channel_layout_combo is not None
        assert self._storage_enabled_checkbox is not None
        assert self._storage_root_edit is not None
        assert self._storage_duration_spin is not None
        assert self._storage_output_data_type_combo is not None
        assert self._storage_int32_gain_spin is not None
        assert self._storage_network_edit is not None
        assert self._storage_station_edit is not None
        assert self._storage_location_edit is not None
        assert self._storage_channel_codes_table is not None
        assert self._datalink_enabled_checkbox is not None
        assert self._datalink_host_edit is not None
        assert self._datalink_port_spin is not None
        assert self._datalink_stream_template_edit is not None
        assert self._datalink_ack_checkbox is not None
        assert self._datalink_send_data2_checkbox is not None
        assert self._capture_enabled_checkbox is not None
        assert self._capture_path_edit is not None
        assert self._gnss_enabled_checkbox is not None
        assert self._gnss_port_combo is not None
        assert self._gnss_baudrate_spin is not None
        assert self._gnss_mode_combo is not None
        assert self._gnss_poll_spin is not None
        assert self._gnss_timestamp_interval_spin is not None

        try:
            channel_codes = self._channel_codes_from_table()
        except ValueError as exc:
            self._set_feedback(f"应用失败: {exc}", is_error=True)
            QtWidgets.QMessageBox.critical(self, "应用配置失败", str(exc))
            return

        payload = {
            "processing": {
                "data1_rate": self._data1_rate_spin.value(),
                "data2_rate": self._data2_rate_spin.value(),
            },
            "data_server": {
                "mode": self._data_server_mode_combo.currentData(),
                "host": self._data_host_edit.text().strip() or "0.0.0.0",
                "port": self._data_port_spin.value(),
                "remote_host": self._data_remote_host_edit.text().strip() or "127.0.0.1",
                "remote_port": self._data_remote_port_spin.value(),
            },
            "protocol": {
                "frame_header": self._frame_header_edit.text().strip() or "11",
                "frame_header_size": int(self._frame_header_size_combo.currentText()),
                "length_field_size": int(self._length_field_size_combo.currentText()),
                "length_field_format": self._length_field_format_combo.currentData(),
                "length_field_units": self._length_field_units_combo.currentData(),
                "byte_order": self._byte_order_combo.currentData(),
                "channel_layout": self._channel_layout_combo.currentData(),
            },
            "storage": {
                "enabled": self._storage_enabled_checkbox.isChecked(),
                "root": self._storage_root_edit.text().strip() or r"E:\data",
                "file_duration_seconds": self._storage_duration_spin.value(),
                "output_data_type": self._storage_output_data_type_combo.currentData(),
                "int32_gain": self._storage_int32_gain_spin.value(),
                "network": self._storage_network_edit.text().strip() or "SC",
                "station": self._storage_station_edit.text().strip() or "S0001",
                "location": self._storage_location_edit.text().strip() or "10",
                "channel_codes": channel_codes,
            },
            "datalink": {
                "enabled": self._datalink_enabled_checkbox.isChecked(),
                "host": self._datalink_host_edit.text().strip() or "10.2.16.61",
                "port": self._datalink_port_spin.value(),
                "stream_id_template": self._datalink_stream_template_edit.text().strip()
                or "{network}_{station}_{location}_{channel}/MSEED",
                "ack_required": self._datalink_ack_checkbox.isChecked(),
                "send_data2": self._datalink_send_data2_checkbox.isChecked(),
            },
            "capture": {
                "enabled": self._capture_enabled_checkbox.isChecked(),
                "path": self._capture_path_edit.text().strip() or "./var/captures/session.dlhcap",
            },
            "gnss": {
                "enabled": self._gnss_enabled_checkbox.isChecked(),
                "port": self._gnss_port_combo.currentText().strip(),
                "baudrate": self._gnss_baudrate_spin.value(),
                "mode": self._gnss_mode_combo.currentData(),
                "poll_interval_seconds": self._gnss_poll_spin.value(),
                "packet_timestamp_timeout_seconds": self._gnss_timestamp_interval_spin.value(),
            },
        }
        try:
            self._runtime.update_config(payload)
            self._set_feedback("配置已应用。")
            self.statusBar().showMessage("配置已应用", 3000)
            self._update_processing_controls()
        except Exception as exc:  # noqa: BLE001
            self._set_feedback(f"应用失败: {exc}", is_error=True)
            QtWidgets.QMessageBox.critical(self, "应用配置失败", str(exc))

    def _start_processing(self) -> None:
        self._runtime.resume_processing()
        self._update_processing_controls()
        self.statusBar().showMessage("数据接收已启动", 3000)

    def _pause_processing(self) -> None:
        self._runtime.pause_processing()
        self._update_processing_controls()
        self.statusBar().showMessage("数据接收已停止", 3000)

    def _set_feedback(self, text: str, *, is_error: bool = False) -> None:
        if self._config_feedback_label is None:
            return
        color = "#b3261e" if is_error else "#1d4ed8"
        self._config_feedback_label.setText(text)
        self._config_feedback_label.setStyleSheet(f"color: {color};")

    def _refresh(self) -> None:
        snapshot = self._runtime.snapshot()
        self._update_processing_controls()
        self._update_status(snapshot)
        data, sample_rate = self._snapshot_data(snapshot)
        x_values = None
        if (
            data is not None
            and data.shape[1] > 0
            and sample_rate is not None
            and sample_rate > 0
        ):
            x_values = (np.arange(data.shape[1]) - data.shape[1] + 1) / sample_rate
        for index, curve in enumerate(self._plots):
            if curve is None:
                continue
            if data is None or data.shape[0] <= index or data.shape[1] == 0:
                curve.setData([])
                continue
            curve.setData(x_values if x_values is not None else np.arange(data.shape[1]), data[index])
        self._update_logs()

    @staticmethod
    def _format_bytes(value: int | None) -> str:
        if value is None:
            return "-"
        units = ("B", "KB", "MB", "GB", "TB", "PB")
        size = float(value)
        unit_index = 0
        while size >= 1024.0 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1
        if unit_index == 0:
            return f"{int(size)} {units[unit_index]}"
        return f"{size:.1f} {units[unit_index]}"

    def _update_status(self, snapshot: RuntimeSnapshot) -> None:
        values = {
            "data_connected": "已连接" if snapshot.data_connected else "未连接",
            "control_connected": "已连接" if snapshot.control_connected else "未连接",
            "source_sample_rate": "-" if snapshot.source_sample_rate is None else f"{snapshot.source_sample_rate:.2f} Hz",
            "packets_received": f"{snapshot.packets_received:,}",
            "bytes_received": f"{snapshot.bytes_received:,}",
            "frames_dropped": f"{snapshot.frames_dropped:,}",
            "queue_depth": str(snapshot.queue_depth),
            "storage_enabled": "已启用" if snapshot.storage_enabled else "未启用",
            "data1_rate": f"{snapshot.data1_rate:.2f} Hz",
            "data2_rate": f"{snapshot.data2_rate:.2f} Hz",
            "storage_queue_depth": str(snapshot.storage_queue_depth),
            "storage_frames_dropped": f"{snapshot.storage_frames_dropped:,}",
            "storage_last_error": snapshot.storage_last_error or "-",
            "storage_disk_total_bytes": self._format_bytes(snapshot.storage_disk_total_bytes),
            "storage_disk_used_bytes": self._format_bytes(snapshot.storage_disk_used_bytes),
            "storage_disk_free_bytes": self._format_bytes(snapshot.storage_disk_free_bytes),
            "storage_disk_usage_percent": (
                "-" if snapshot.storage_disk_usage_percent is None else f"{snapshot.storage_disk_usage_percent:.1f}%"
            ),
            "datalink_enabled": "已启用" if snapshot.datalink_enabled else "未启用",
            "datalink_connected": "已连接" if snapshot.datalink_connected else "未连接",
            "datalink_packets_sent": f"{snapshot.datalink_packets_sent:,}",
            "datalink_bytes_sent": f"{snapshot.datalink_bytes_sent:,}",
            "datalink_reconnects": f"{snapshot.datalink_reconnects:,}",
            "datalink_last_error": snapshot.datalink_last_error or "-",
            "datalink_publish_queue_depth": str(snapshot.datalink_publish_queue_depth),
            "datalink_publish_frames_dropped": f"{snapshot.datalink_publish_frames_dropped:,}",
            "datalink_publish_last_error": snapshot.datalink_publish_last_error or "-",
            "capture_enabled": "已启用" if snapshot.capture_enabled else "未启用",
            "gnss_enabled": "已启用" if snapshot.gnss_enabled else "未启用",
            "gnss_connected": "已连接" if snapshot.gnss_connected else "未连接",
            "gnss_mode": snapshot.gnss_mode,
            "gnss_port": snapshot.gnss_port or "-",
            "gnss_last_timestamp": snapshot.gnss_last_timestamp or "-",
            "gnss_last_error": snapshot.gnss_last_error or "-",
            "gnss_fallback_active": "是" if snapshot.gnss_fallback_active else "否",
            "last_error": snapshot.last_error or "-",
        }
        for key, labels in self._status_labels.items():
            value = values.get(key, "-")
            for label in labels:
                label.setText(value)

        bool_values = {
            "data_connected": snapshot.data_connected,
            "control_connected": snapshot.control_connected,
            "storage_enabled": snapshot.storage_enabled,
            "datalink_enabled": snapshot.datalink_enabled,
            "datalink_connected": snapshot.datalink_connected,
            "gnss_enabled": snapshot.gnss_enabled,
            "gnss_connected": snapshot.gnss_connected,
            "capture_enabled": snapshot.capture_enabled,
        }
        for key, lamps in self._status_lamps.items():
            active = bool_values.get(key, False)
            for lamp in lamps:
                lamp.set_active(active)

        self._update_queue_bar("processing", snapshot.queue_depth, PROCESSING_QUEUE_CAPACITY)
        self._update_queue_bar("storage", snapshot.storage_queue_depth, STORAGE_QUEUE_CAPACITY)
        self._update_queue_bar("datalink", snapshot.datalink_publish_queue_depth, DATALINK_QUEUE_CAPACITY)

        if self._gnss_last_timestamp_label is not None:
            self._gnss_last_timestamp_label.setText(snapshot.gnss_last_timestamp or "-")
        if self._gnss_last_error_label is not None:
            self._gnss_last_error_label.setText(snapshot.gnss_last_error or "-")
        if self._remote_web_label is not None:
            self._remote_web_label.setText(f"http://{self._settings.web.host}:{self._settings.web.port}")
        if self._remote_control_label is not None:
            self._remote_control_label.setText(f"{self._settings.control_server.host}:{self._settings.control_server.port}")

    def _update_queue_bar(self, key: str, value: int, capacity: int) -> None:
        bar = self._queue_bars.get(key)
        label = self._queue_value_labels.get(key)
        if bar is None or label is None:
            return
        clamped_value = min(max(value, 0), capacity)
        bar.setValue(clamped_value)
        label.setText(f"{value} / {capacity}")
        ratio = 0.0 if capacity <= 0 else value / capacity
        if ratio >= 0.75:
            chunk = "#d16363"
        elif ratio >= 0.4:
            chunk = "#d1a252"
        else:
            chunk = "#6b8fd6"
        bar.setStyleSheet(
            "QProgressBar {"
            "border:1px solid #93a6c5; background:#eef3fb; border-radius:4px; min-height:18px;"
            "}"
            f"QProgressBar::chunk {{ background:{chunk}; border-radius:4px; }}"
        )

    def _update_processing_controls(self) -> None:
        if (
            self._processing_state_label is None
            or self._start_processing_button is None
            or self._pause_processing_button is None
            or self._ingest_help_label is None
        ):
            return
        active = self._runtime.is_processing_active()
        self._processing_state_label.setText("数据接收状态: 运行中" if active else "数据接收状态: 已停止")
        self._start_processing_button.setEnabled(not active)
        self._pause_processing_button.setEnabled(active)
        self._ingest_help_label.setText(
            "当前运行中，应用配置后会按新参数自动重连。" if active else "当前未运行，应用配置后只更新参数，不会自动启动。"
        )

    def _snapshot_data(self, snapshot: RuntimeSnapshot) -> tuple[np.ndarray | None, float | None]:
        def window(
            data: np.ndarray | None,
            sample_rate: float | None,
        ) -> tuple[np.ndarray | None, float | None]:
            max_points = self._plot_window_points(sample_rate)
            windowed = slice_for_plot(data, max_points)
            display_limit = max(int(self._settings.gui.display_max_points_per_trace), 1)
            plotted, step = downsample_for_plot(windowed, display_limit)
            effective_rate = sample_rate / step if sample_rate is not None and sample_rate > 0 else sample_rate
            return plotted, effective_rate

        if self._data_mode == "raw":
            return window(snapshot.latest_raw, snapshot.source_sample_rate)
        if self._data_mode == "data1":
            return window(snapshot.latest_data1, snapshot.data1_rate)
        if self._data_mode == "data2":
            return window(snapshot.latest_data2, snapshot.data2_rate)
        return window(snapshot.latest_raw, snapshot.source_sample_rate)

    def _plot_window_points(self, sample_rate: float | None) -> int:
        seconds = (
            self._plot_window_seconds_spin.value()
            if self._plot_window_seconds_spin is not None
            else self._settings.gui.plot_window_seconds
        )
        point_limit = max(int(self._settings.gui.max_points_per_trace), 1)
        if sample_rate is None or sample_rate <= 0:
            return point_limit
        return max(1, min(point_limit, int(round(sample_rate * max(seconds, 1.0)))))

    def _update_logs(self) -> None:
        if self._log_view is None or self._log_level_combo is None:
            return
        level = self._log_level_combo.currentText()
        lines = get_recent_logs(1000)
        level_map = {
            "全部": None,
            "信息": "INFO",
            "警告": "WARNING",
            "错误": "ERROR",
        }
        selected = level_map[level]
        if selected is not None:
            lines = [line for line in lines if f" {selected} " in line]
        text = "\n".join(lines)
        if text != self._log_view.toPlainText():
            self._log_view.setPlainText(text)
            cursor = self._log_view.textCursor()
            cursor.movePosition(cursor.MoveOperation.End)
            self._log_view.setTextCursor(cursor)

    def _channel_color(self, channel_index: int) -> str:
        colors = [
            "#1f4e79",
            "#4f81bd",
            "#2b6cb0",
            "#7a9fd6",
            "#9d3c49",
            "#d17b88",
            "#557a46",
            "#8eb274",
        ]
        return colors[channel_index % len(colors)]

    def _apply_styles(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow, QWidget {
                background: #f4f6fa;
                color: #121620;
                font-size: 14px;
                font-family: "Songti SC", "STSong", "Noto Serif CJK SC", "PingFang SC";
            }
            QFrame#shell {
                background: #eef2f8;
                border: 2px solid #314d7b;
            }
            QFrame#banner {
                background: #b9c8e8;
                border: 1px solid #314d7b;
                min-height: 108px;
            }
            QLabel#bannerTitle {
                font-size: 29px;
                font-weight: 600;
                letter-spacing: 1px;
            }
            QTabWidget::pane {
                border: 1px solid #314d7b;
                background: #f7f9fc;
                margin-top: -1px;
            }
            QTabBar::tab {
                background: #edf2fa;
                border: 1px solid #314d7b;
                padding: 8px 18px;
                min-width: 98px;
            }
            QTabBar::tab:selected {
                background: #ffffff;
                border-bottom-color: #ffffff;
            }
            QFrame#introStrip, QFrame#controlStrip, QFrame#workspace {
                background: #f7f9fc;
                border: 1px solid #314d7b;
            }
            QLabel#pageTitle {
                font-size: 18px;
                font-weight: 600;
            }
            QLabel#mutedText {
                color: #44556f;
            }
            QLabel#feedbackText {
                color: #1d4ed8;
                font-weight: 500;
            }
            QLabel#stateText {
                font-size: 16px;
                font-weight: 600;
            }
            QLabel#statusValue {
                color: #1d2636;
            }
            QLabel#laneLabel {
                min-width: 88px;
                color: #1c2f4b;
                font-size: 15px;
            }
            QGroupBox#panelCard {
                background: #ffffff;
                border: 1px solid #2d3138;
                margin-top: 22px;
                font-size: 17px;
                font-weight: 600;
            }
            QGroupBox#panelCard::title {
                subcontrol-origin: margin;
                left: 12px;
                top: 4px;
                padding: 0 6px;
            }
            QFrame#plotCard {
                background: #ffffff;
                border: 1px solid #2d3138;
            }
            QLabel#plotCardTitle {
                font-size: 18px;
                font-weight: 600;
                color: #162336;
            }
            QLabel#queueValue {
                color: #314d7b;
                font-weight: 600;
            }
            QPushButton {
                background: #e8eef9;
                border: 1px solid #7f93b4;
                padding: 6px 12px;
                min-height: 28px;
            }
            QPushButton:hover {
                background: #dbe6fb;
            }
            QPushButton:pressed {
                background: #cedcf4;
            }
            QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox, QPlainTextEdit {
                background: #fbfcff;
                border: 1px solid #9fb0ca;
                padding: 4px 6px;
                selection-background-color: #9db7e6;
            }
            QCheckBox {
                spacing: 8px;
            }
            QScrollArea {
                border: none;
                background: transparent;
            }
            QStatusBar {
                background: #e5ebf6;
                border-top: 1px solid #9eb0cc;
            }
            """
        )
