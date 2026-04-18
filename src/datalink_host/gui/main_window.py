from __future__ import annotations

from functools import partial

import numpy as np
import pyqtgraph as pg
from PySide6 import QtCore, QtWidgets

from datalink_host.core.config import AppSettings
from datalink_host.core.logging import get_recent_logs
from datalink_host.models.messages import RuntimeSnapshot
from datalink_host.processing.pipeline import compute_psd
from datalink_host.services.runtime import RuntimeService, slice_for_plot


def recommended_window_size(available_width: int, available_height: int) -> tuple[int, int]:
    width = min(1440, max(1080, int(available_width * 0.92)))
    height = min(920, max(760, int(available_height * 0.92)))
    return min(width, available_width), min(height, available_height)


def _status_value_label(parent: QtWidgets.QWidget) -> QtWidgets.QLabel:
    label = QtWidgets.QLabel("-", parent)
    label.setWordWrap(True)
    label.setTextInteractionFlags(QtCore.Qt.TextInteractionFlag.TextSelectableByMouse)
    label.setSizePolicy(
        QtWidgets.QSizePolicy.Policy.Expanding,
        QtWidgets.QSizePolicy.Policy.Preferred,
    )
    return label


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, runtime: RuntimeService, settings: AppSettings) -> None:
        super().__init__()
        self._runtime = runtime
        self._settings = settings
        self._data_mode = "unwrapped"
        self._status_labels: dict[str, QtWidgets.QLabel] = {}
        self._plots: list[pg.PlotDataItem] = []

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
        self._storage_network_edit: QtWidgets.QLineEdit | None = None
        self._storage_station_edit: QtWidgets.QLineEdit | None = None
        self._storage_location_edit: QtWidgets.QLineEdit | None = None

        self._datalink_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._datalink_host_edit: QtWidgets.QLineEdit | None = None
        self._datalink_port_spin: QtWidgets.QSpinBox | None = None
        self._datalink_ack_checkbox: QtWidgets.QCheckBox | None = None
        self._datalink_send_data2_checkbox: QtWidgets.QCheckBox | None = None

        self._capture_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._capture_path_edit: QtWidgets.QLineEdit | None = None
        self._capture_browse_button: QtWidgets.QPushButton | None = None

        self._gps_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._gps_port_combo: QtWidgets.QComboBox | None = None
        self._gps_refresh_button: QtWidgets.QPushButton | None = None
        self._gps_baudrate_spin: QtWidgets.QSpinBox | None = None
        self._gps_mode_combo: QtWidgets.QComboBox | None = None
        self._gps_poll_spin: QtWidgets.QDoubleSpinBox | None = None

        self._analysis_channel_spin: QtWidgets.QSpinBox | None = None
        self._analysis_window_spin: QtWidgets.QSpinBox | None = None
        self._analysis_time_curve: pg.PlotDataItem | None = None
        self._analysis_psd_curve: pg.PlotDataItem | None = None
        self._log_view: QtWidgets.QPlainTextEdit | None = None
        self._log_level_combo: QtWidgets.QComboBox | None = None

        self.setWindowTitle("长基线光纤应变信号监控软件")
        self.statusBar().showMessage("就绪")
        self._build_ui()
        self._configure_window_geometry()
        self._load_runtime_config_into_form()
        self._refresh_gps_ports()

        self._timer = QtCore.QTimer(self)
        self._timer.timeout.connect(self._refresh)
        self._timer.start(settings.gui.refresh_interval_ms)

    def _build_ui(self) -> None:
        tabs = QtWidgets.QTabWidget(self)
        tabs.addTab(self._build_overview_tab(), "总览")
        tabs.addTab(self._build_config_tab(), "配置")
        tabs.addTab(self._build_analysis_tab(), "分析")
        tabs.addTab(self._build_logs_tab(), "日志")
        self.setCentralWidget(tabs)

    def _build_overview_tab(self) -> QtWidgets.QWidget:
        widget = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(widget)
        layout.addWidget(self._build_action_panel())
        layout.addWidget(self._build_status_panel())
        layout.addWidget(self._build_wave_panel(), stretch=1)
        return widget

    def _build_action_panel(self) -> QtWidgets.QWidget:
        panel = QtWidgets.QGroupBox("运行操作", self)
        layout = QtWidgets.QVBoxLayout(panel)

        mode_row = QtWidgets.QHBoxLayout()
        mode_row.addWidget(QtWidgets.QLabel("当前查看:", panel))
        button_group = QtWidgets.QButtonGroup(self)
        for name, text in (
            ("raw", "原始"),
            ("unwrapped", "相位展开"),
            ("data1", "降采样1"),
            ("data2", "降采样2"),
        ):
            button = QtWidgets.QRadioButton(text, panel)
            if name == self._data_mode:
                button.setChecked(True)
            button.toggled.connect(partial(self._set_mode, name))
            button_group.addButton(button)
            mode_row.addWidget(button)
        mode_row.addStretch(1)

        action_row = QtWidgets.QHBoxLayout()
        self._processing_state_label = QtWidgets.QLabel(panel)
        self._start_processing_button = QtWidgets.QPushButton("启动数据接收", panel)
        self._pause_processing_button = QtWidgets.QPushButton("停止数据接收", panel)
        self._reload_button = QtWidgets.QPushButton("重载配置到界面", panel)
        self._start_processing_button.clicked.connect(self._start_processing)
        self._pause_processing_button.clicked.connect(self._pause_processing)
        self._reload_button.clicked.connect(self._load_runtime_config_into_form)
        action_row.addWidget(self._processing_state_label)
        action_row.addStretch(1)
        action_row.addWidget(self._reload_button)
        action_row.addWidget(self._start_processing_button)
        action_row.addWidget(self._pause_processing_button)

        self._ingest_help_label = QtWidgets.QLabel(panel)
        self._ingest_help_label.setWordWrap(True)

        layout.addLayout(mode_row)
        layout.addLayout(action_row)
        layout.addWidget(self._ingest_help_label)
        self._update_processing_controls()
        return panel

    def _build_status_panel(self) -> QtWidgets.QWidget:
        panel = QtWidgets.QWidget(self)
        layout = QtWidgets.QGridLayout(panel)
        layout.addWidget(
            self._build_status_group(
                "连接与处理",
                [
                    ("data_connected", "数据连接"),
                    ("control_connected", "控制连接"),
                    ("source_sample_rate", "源采样率"),
                    ("packets_received", "已收包数"),
                    ("bytes_received", "已收字节"),
                    ("queue_depth", "处理队列"),
                    ("frames_dropped", "处理丢帧"),
                ],
            ),
            0,
            0,
        )
        layout.addWidget(
            self._build_status_group(
                "输出链路",
                [
                    ("storage_enabled", "本地存储"),
                    ("data1_rate", "降采样1"),
                    ("data2_rate", "降采样2"),
                    ("storage_queue_depth", "存储队列"),
                    ("storage_frames_dropped", "存储丢帧"),
                    ("datalink_enabled", "远传开关"),
                    ("datalink_connected", "远传连接"),
                    ("datalink_packets_sent", "远传已发包"),
                    ("datalink_bytes_sent", "远传已发字节"),
                    ("datalink_reconnects", "远传重连"),
                    ("datalink_publish_queue_depth", "远传发布队列"),
                    ("datalink_publish_frames_dropped", "远传发布丢帧"),
                ],
            ),
            0,
            1,
        )
        layout.addWidget(
            self._build_status_group(
                "时间与异常",
                [
                    ("capture_enabled", "抓包"),
                    ("gps_enabled", "GPS"),
                    ("gps_connected", "GPS连接"),
                    ("gps_mode", "GPS模式"),
                    ("gps_port", "GPS串口"),
                    ("gps_last_timestamp", "GPS时间"),
                    ("gps_last_error", "GPS错误"),
                    ("gps_fallback_active", "GPS回退"),
                    ("last_error", "最近错误"),
                    ("storage_last_error", "存储错误"),
                    ("datalink_last_error", "远传错误"),
                    ("datalink_publish_last_error", "远传发布错误"),
                ],
            ),
            0,
            2,
        )
        layout.setColumnStretch(0, 1)
        layout.setColumnStretch(1, 1)
        layout.setColumnStretch(2, 1)
        return panel

    def _build_status_group(
        self,
        title: str,
        fields: list[tuple[str, str]],
    ) -> QtWidgets.QWidget:
        group = QtWidgets.QGroupBox(title, self)
        form = QtWidgets.QFormLayout(group)
        form.setFieldGrowthPolicy(QtWidgets.QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        for key, label_text in fields:
            label = _status_value_label(group)
            self._status_labels[key] = label
            form.addRow(label_text, label)
        return group

    def _build_wave_panel(self) -> QtWidgets.QWidget:
        panel = QtWidgets.QGroupBox("通道总览", self)
        layout = QtWidgets.QGridLayout(panel)
        colors = [
            "#1f77b4",
            "#ff7f0e",
            "#2ca02c",
            "#d62728",
            "#9467bd",
            "#8c564b",
            "#17becf",
            "#bcbd22",
        ]
        for channel in range(self._settings.protocol.channels):
            plot_widget = pg.PlotWidget(title=f"CH {channel + 1}")
            plot_widget.showGrid(x=True, y=True, alpha=0.2)
            plot_widget.setBackground("w")
            curve = plot_widget.plot(pen=pg.mkPen(colors[channel % len(colors)], width=1.6))
            self._plots.append(curve)
            layout.addWidget(plot_widget, channel // 2, channel % 2)
        return panel

    def _build_config_tab(self) -> QtWidgets.QWidget:
        scroll = QtWidgets.QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)

        container = QtWidgets.QWidget(scroll)
        layout = QtWidgets.QVBoxLayout(container)
        layout.addWidget(self._build_config_header())
        layout.addWidget(self._build_processing_section())
        layout.addWidget(self._build_ingest_section())
        layout.addWidget(self._build_protocol_section())
        layout.addWidget(self._build_storage_section())
        layout.addWidget(self._build_datalink_section())
        layout.addWidget(self._build_capture_section())
        layout.addWidget(self._build_gps_section())
        layout.addStretch(1)

        scroll.setWidget(container)
        return scroll

    def _build_config_header(self) -> QtWidgets.QWidget:
        widget = QtWidgets.QGroupBox("配置流程", self)
        layout = QtWidgets.QVBoxLayout(widget)

        summary = QtWidgets.QLabel(
            "先在下面分组编辑参数，再点击“应用配置”。如果当前正在接收，应用后会按新配置自动重建连接；"
            "如果当前已停止，则只更新参数，不会自动启动。",
            widget,
        )
        summary.setWordWrap(True)

        row = QtWidgets.QHBoxLayout()
        self._config_feedback_label = QtWidgets.QLabel("表单已同步到当前运行时配置。", widget)
        self._config_feedback_label.setWordWrap(True)
        self._apply_button = QtWidgets.QPushButton("应用配置", widget)
        self._apply_button.clicked.connect(self._apply_runtime_config)
        row.addWidget(self._config_feedback_label, stretch=1)
        row.addWidget(self._apply_button)

        layout.addWidget(summary)
        layout.addLayout(row)
        return widget

    def _build_processing_section(self) -> QtWidgets.QWidget:
        group = QtWidgets.QGroupBox("处理参数", self)
        form = QtWidgets.QFormLayout(group)

        self._data1_rate_spin = QtWidgets.QDoubleSpinBox(group)
        self._data1_rate_spin.setRange(0.1, 10000.0)
        self._data1_rate_spin.setDecimals(2)

        self._data2_rate_spin = QtWidgets.QDoubleSpinBox(group)
        self._data2_rate_spin.setRange(0.1, 10000.0)
        self._data2_rate_spin.setDecimals(2)

        form.addRow("降采样1 采样率", self._data1_rate_spin)
        form.addRow("降采样2 采样率", self._data2_rate_spin)
        return group

    def _build_ingest_section(self) -> QtWidgets.QWidget:
        group = QtWidgets.QGroupBox("数据接入", self)
        layout = QtWidgets.QVBoxLayout(group)
        form = QtWidgets.QFormLayout()

        self._data_server_mode_combo = QtWidgets.QComboBox(group)
        self._data_server_mode_combo.addItem("主动连接设备", "client")
        self._data_server_mode_combo.addItem("监听设备连接", "server")
        self._data_server_mode_combo.currentIndexChanged.connect(self._update_form_state)

        self._data_host_edit = QtWidgets.QLineEdit(group)
        self._data_port_spin = QtWidgets.QSpinBox(group)
        self._data_port_spin.setRange(1, 65535)
        self._data_remote_host_edit = QtWidgets.QLineEdit(group)
        self._data_remote_port_spin = QtWidgets.QSpinBox(group)
        self._data_remote_port_spin.setRange(1, 65535)

        form.addRow("接入模式", self._data_server_mode_combo)
        form.addRow("本地监听地址", self._data_host_edit)
        form.addRow("本地监听端口", self._data_port_spin)
        form.addRow("设备地址", self._data_remote_host_edit)
        form.addRow("设备端口", self._data_remote_port_spin)

        self._connection_mode_hint_label = QtWidgets.QLabel(group)
        self._connection_mode_hint_label.setWordWrap(True)

        layout.addLayout(form)
        layout.addWidget(self._connection_mode_hint_label)
        return group

    def _build_protocol_section(self) -> QtWidgets.QWidget:
        group = QtWidgets.QGroupBox("协议解析", self)
        form = QtWidgets.QFormLayout(group)

        self._frame_header_edit = QtWidgets.QLineEdit(group)
        self._frame_header_size_combo = QtWidgets.QComboBox(group)
        self._frame_header_size_combo.addItems(["2", "4", "8"])
        self._length_field_size_combo = QtWidgets.QComboBox(group)
        self._length_field_size_combo.addItems(["4", "8"])
        self._length_field_format_combo = QtWidgets.QComboBox(group)
        self._length_field_format_combo.addItem("无符号整数", "uint")
        self._length_field_format_combo.addItem("浮点 float64", "float64")
        self._length_field_units_combo = QtWidgets.QComboBox(group)
        self._length_field_units_combo.addItem("字节", "bytes")
        self._length_field_units_combo.addItem("数值个数", "values")
        self._byte_order_combo = QtWidgets.QComboBox(group)
        self._byte_order_combo.addItem("大端", "big")
        self._byte_order_combo.addItem("小端", "little")
        self._channel_layout_combo = QtWidgets.QComboBox(group)
        self._channel_layout_combo.addItem("采样交织", "interleaved")
        self._channel_layout_combo.addItem("按通道连续", "channel-major")

        form.addRow("帧头值", self._frame_header_edit)
        form.addRow("帧头字节数", self._frame_header_size_combo)
        form.addRow("长度字段字节数", self._length_field_size_combo)
        form.addRow("长度字段格式", self._length_field_format_combo)
        form.addRow("长度单位", self._length_field_units_combo)
        form.addRow("字节序", self._byte_order_combo)
        form.addRow("通道排列", self._channel_layout_combo)
        return group

    def _build_storage_section(self) -> QtWidgets.QWidget:
        group = QtWidgets.QGroupBox("本地存储", self)
        layout = QtWidgets.QVBoxLayout(group)
        form = QtWidgets.QFormLayout()

        self._storage_enabled_checkbox = QtWidgets.QCheckBox("启用本地存储", group)
        self._storage_enabled_checkbox.toggled.connect(self._update_form_state)
        self._storage_root_edit = QtWidgets.QLineEdit(group)
        self._storage_browse_button = QtWidgets.QPushButton("浏览...", group)
        self._storage_browse_button.clicked.connect(self._choose_storage_root)
        storage_root_row = QtWidgets.QHBoxLayout()
        storage_root_row.addWidget(self._storage_root_edit, stretch=1)
        storage_root_row.addWidget(self._storage_browse_button)
        storage_root_widget = QtWidgets.QWidget(group)
        storage_root_widget.setLayout(storage_root_row)

        self._storage_duration_spin = QtWidgets.QSpinBox(group)
        self._storage_duration_spin.setRange(1, 86400)
        self._storage_network_edit = QtWidgets.QLineEdit(group)
        self._storage_station_edit = QtWidgets.QLineEdit(group)
        self._storage_location_edit = QtWidgets.QLineEdit(group)

        layout.addWidget(self._storage_enabled_checkbox)
        form.addRow("存储目录", storage_root_widget)
        form.addRow("单文件时长(秒)", self._storage_duration_spin)
        form.addRow("网络码", self._storage_network_edit)
        form.addRow("台站码", self._storage_station_edit)
        form.addRow("位置码", self._storage_location_edit)
        layout.addLayout(form)
        return group

    def _build_datalink_section(self) -> QtWidgets.QWidget:
        group = QtWidgets.QGroupBox("DataLink 远传", self)
        layout = QtWidgets.QVBoxLayout(group)
        form = QtWidgets.QFormLayout()

        self._datalink_enabled_checkbox = QtWidgets.QCheckBox("启用 DataLink 远传", group)
        self._datalink_enabled_checkbox.toggled.connect(self._update_form_state)
        self._datalink_host_edit = QtWidgets.QLineEdit(group)
        self._datalink_port_spin = QtWidgets.QSpinBox(group)
        self._datalink_port_spin.setRange(1, 65535)
        self._datalink_ack_checkbox = QtWidgets.QCheckBox("发送后等待 ACK", group)
        self._datalink_send_data2_checkbox = QtWidgets.QCheckBox("同时发送降采样2", group)

        layout.addWidget(self._datalink_enabled_checkbox)
        form.addRow("远传主机", self._datalink_host_edit)
        form.addRow("远传端口", self._datalink_port_spin)
        form.addRow("确认策略", self._datalink_ack_checkbox)
        form.addRow("发送内容", self._datalink_send_data2_checkbox)
        layout.addLayout(form)
        return group

    def _build_capture_section(self) -> QtWidgets.QWidget:
        group = QtWidgets.QGroupBox("抓包", self)
        layout = QtWidgets.QVBoxLayout(group)
        form = QtWidgets.QFormLayout()

        self._capture_enabled_checkbox = QtWidgets.QCheckBox("启用原始 TCP 抓包", group)
        self._capture_enabled_checkbox.toggled.connect(self._update_form_state)
        self._capture_path_edit = QtWidgets.QLineEdit(group)
        self._capture_browse_button = QtWidgets.QPushButton("抓包文件...", group)
        self._capture_browse_button.clicked.connect(self._choose_capture_path)
        capture_path_row = QtWidgets.QHBoxLayout()
        capture_path_row.addWidget(self._capture_path_edit, stretch=1)
        capture_path_row.addWidget(self._capture_browse_button)
        capture_path_widget = QtWidgets.QWidget(group)
        capture_path_widget.setLayout(capture_path_row)

        layout.addWidget(self._capture_enabled_checkbox)
        form.addRow("抓包文件", capture_path_widget)
        layout.addLayout(form)
        return group

    def _build_gps_section(self) -> QtWidgets.QWidget:
        group = QtWidgets.QGroupBox("GPS 时间", self)
        layout = QtWidgets.QVBoxLayout(group)
        form = QtWidgets.QFormLayout()

        self._gps_enabled_checkbox = QtWidgets.QCheckBox("启用 GPS 时间", group)
        self._gps_enabled_checkbox.toggled.connect(self._update_form_state)
        self._gps_port_combo = QtWidgets.QComboBox(group)
        self._gps_port_combo.setEditable(True)
        self._gps_port_combo.setInsertPolicy(QtWidgets.QComboBox.InsertPolicy.NoInsert)
        self._gps_refresh_button = QtWidgets.QPushButton("刷新串口", group)
        self._gps_refresh_button.clicked.connect(self._refresh_gps_ports)
        gps_port_row = QtWidgets.QHBoxLayout()
        gps_port_row.addWidget(self._gps_port_combo, stretch=1)
        gps_port_row.addWidget(self._gps_refresh_button)
        gps_port_widget = QtWidgets.QWidget(group)
        gps_port_widget.setLayout(gps_port_row)

        self._gps_baudrate_spin = QtWidgets.QSpinBox(group)
        self._gps_baudrate_spin.setRange(1, 921600)
        self._gps_mode_combo = QtWidgets.QComboBox(group)
        self._gps_mode_combo.addItem("调试模式", "debug")
        self._gps_mode_combo.addItem("部署模式", "deploy")
        self._gps_poll_spin = QtWidgets.QDoubleSpinBox(group)
        self._gps_poll_spin.setRange(0.01, 10.0)
        self._gps_poll_spin.setDecimals(2)

        layout.addWidget(self._gps_enabled_checkbox)
        form.addRow("串口", gps_port_widget)
        form.addRow("波特率", self._gps_baudrate_spin)
        form.addRow("模式", self._gps_mode_combo)
        form.addRow("轮询间隔(秒)", self._gps_poll_spin)
        layout.addLayout(form)
        return group

    def _build_analysis_tab(self) -> QtWidgets.QWidget:
        widget = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(widget)

        controls = QtWidgets.QHBoxLayout()
        self._analysis_channel_spin = QtWidgets.QSpinBox(widget)
        self._analysis_channel_spin.setRange(1, self._settings.protocol.channels)
        self._analysis_channel_spin.setValue(1)
        self._analysis_window_spin = QtWidgets.QSpinBox(widget)
        self._analysis_window_spin.setRange(16, self._settings.gui.max_points_per_trace)
        self._analysis_window_spin.setSingleStep(128)
        self._analysis_window_spin.setValue(min(1024, self._settings.gui.max_points_per_trace))
        controls.addWidget(QtWidgets.QLabel("通道"))
        controls.addWidget(self._analysis_channel_spin)
        controls.addWidget(QtWidgets.QLabel("PSD 样本数"))
        controls.addWidget(self._analysis_window_spin)
        controls.addStretch(1)
        layout.addLayout(controls)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical, widget)
        time_plot = pg.PlotWidget(title="时域波形")
        time_plot.showGrid(x=True, y=True, alpha=0.2)
        time_plot.setBackground("w")
        self._analysis_time_curve = time_plot.plot(pen=pg.mkPen("#136f63", width=1.8))

        psd_plot = pg.PlotWidget(title="功率谱密度 PSD")
        psd_plot.showGrid(x=True, y=True, alpha=0.2)
        psd_plot.setBackground("w")
        psd_plot.setLogMode(False, True)
        self._analysis_psd_curve = psd_plot.plot(pen=pg.mkPen("#d1495b", width=1.8))

        splitter.addWidget(time_plot)
        splitter.addWidget(psd_plot)
        layout.addWidget(splitter, stretch=1)
        return widget

    def _build_logs_tab(self) -> QtWidgets.QWidget:
        widget = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(widget)

        controls = QtWidgets.QHBoxLayout()
        self._log_level_combo = QtWidgets.QComboBox(widget)
        self._log_level_combo.addItems(["全部", "信息", "警告", "错误"])
        controls.addWidget(QtWidgets.QLabel("筛选"))
        controls.addWidget(self._log_level_combo)
        controls.addStretch(1)
        layout.addLayout(controls)

        self._log_view = QtWidgets.QPlainTextEdit(widget)
        self._log_view.setReadOnly(True)
        layout.addWidget(self._log_view, stretch=1)
        return widget

    def _configure_window_geometry(self) -> None:
        screen = QtWidgets.QApplication.primaryScreen()
        if screen is None:
            self.resize(1280, 860)
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
        assert self._storage_network_edit is not None
        assert self._storage_station_edit is not None
        assert self._storage_location_edit is not None
        assert self._datalink_enabled_checkbox is not None
        assert self._datalink_host_edit is not None
        assert self._datalink_port_spin is not None
        assert self._datalink_ack_checkbox is not None
        assert self._datalink_send_data2_checkbox is not None
        assert self._capture_enabled_checkbox is not None
        assert self._capture_path_edit is not None
        assert self._gps_enabled_checkbox is not None
        assert self._gps_port_combo is not None
        assert self._gps_baudrate_spin is not None
        assert self._gps_mode_combo is not None
        assert self._gps_poll_spin is not None

        processing = config["processing"]
        data_server = config["data_server"]
        protocol = config["protocol"]
        storage = config["storage"]
        datalink = config["datalink"]
        capture = config["capture"]
        gps = config["gps"]

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
        self._storage_network_edit.setText(storage["network"])
        self._storage_station_edit.setText(storage["station"])
        self._storage_location_edit.setText(storage["location"])

        self._datalink_enabled_checkbox.setChecked(datalink["enabled"])
        self._datalink_host_edit.setText(datalink["host"])
        self._datalink_port_spin.setValue(datalink["port"])
        self._datalink_ack_checkbox.setChecked(datalink["ack_required"])
        self._datalink_send_data2_checkbox.setChecked(datalink["send_data2"])

        self._capture_enabled_checkbox.setChecked(capture["enabled"])
        self._capture_path_edit.setText(capture["path"])

        self._gps_enabled_checkbox.setChecked(gps["enabled"])
        self._gps_baudrate_spin.setValue(gps["baudrate"])
        self._gps_mode_combo.setCurrentIndex(0 if gps["mode"] == "debug" else 1)
        self._gps_poll_spin.setValue(gps["poll_interval_seconds"])
        self._refresh_gps_ports(selected=gps["port"])
        self._update_form_state()
        self._set_feedback("表单已同步到当前运行时配置。")

    def _set_mode(self, name: str, checked: bool) -> None:
        if checked:
            self._data_mode = name

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

    def _refresh_gps_ports(self, selected: str | None = None) -> None:
        if self._gps_port_combo is None:
            return
        current = (selected if selected is not None else self._gps_port_combo.currentText()).strip()
        ports = self._runtime.gps_ports()
        self._gps_port_combo.blockSignals(True)
        self._gps_port_combo.clear()
        for port in ports:
            self._gps_port_combo.addItem(port)
        if current:
            if current not in ports:
                self._gps_port_combo.addItem(current)
            self._gps_port_combo.setCurrentText(current)
        self._gps_port_combo.blockSignals(False)

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
            self._storage_enabled_checkbox.isChecked() if self._storage_enabled_checkbox is not None else False,
            [
                self._storage_root_edit,
                self._storage_browse_button,
                self._storage_duration_spin,
                self._storage_network_edit,
                self._storage_station_edit,
                self._storage_location_edit,
            ],
        )
        self._set_section_enabled(
            self._datalink_enabled_checkbox.isChecked() if self._datalink_enabled_checkbox is not None else False,
            [
                self._datalink_host_edit,
                self._datalink_port_spin,
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
            self._gps_enabled_checkbox.isChecked() if self._gps_enabled_checkbox is not None else False,
            [
                self._gps_port_combo,
                self._gps_refresh_button,
                self._gps_baudrate_spin,
                self._gps_mode_combo,
                self._gps_poll_spin,
            ],
        )
        self._update_processing_controls()

    @staticmethod
    def _set_section_enabled(enabled: bool, widgets: list[QtWidgets.QWidget | None]) -> None:
        for widget in widgets:
            if widget is not None:
                widget.setEnabled(enabled)

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
        assert self._storage_network_edit is not None
        assert self._storage_station_edit is not None
        assert self._storage_location_edit is not None
        assert self._datalink_enabled_checkbox is not None
        assert self._datalink_host_edit is not None
        assert self._datalink_port_spin is not None
        assert self._datalink_ack_checkbox is not None
        assert self._datalink_send_data2_checkbox is not None
        assert self._capture_enabled_checkbox is not None
        assert self._capture_path_edit is not None
        assert self._gps_enabled_checkbox is not None
        assert self._gps_port_combo is not None
        assert self._gps_baudrate_spin is not None
        assert self._gps_mode_combo is not None
        assert self._gps_poll_spin is not None

        payload = {
            "processing": {
                "data1_rate": self._data1_rate_spin.value(),
                "data2_rate": self._data2_rate_spin.value(),
            },
            "data_server": {
                "mode": self._data_server_mode_combo.currentData(),
                "host": self._data_host_edit.text().strip() or "0.0.0.0",
                "port": self._data_port_spin.value(),
                "remote_host": self._data_remote_host_edit.text().strip() or "169.254.56.252",
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
                "root": self._storage_root_edit.text().strip() or "./var/storage",
                "file_duration_seconds": self._storage_duration_spin.value(),
                "network": self._storage_network_edit.text().strip() or "SC",
                "station": self._storage_station_edit.text().strip() or "S0001",
                "location": self._storage_location_edit.text().strip() or "10",
            },
            "datalink": {
                "enabled": self._datalink_enabled_checkbox.isChecked(),
                "host": self._datalink_host_edit.text().strip() or "127.0.0.1",
                "port": self._datalink_port_spin.value(),
                "ack_required": self._datalink_ack_checkbox.isChecked(),
                "send_data2": self._datalink_send_data2_checkbox.isChecked(),
            },
            "capture": {
                "enabled": self._capture_enabled_checkbox.isChecked(),
                "path": self._capture_path_edit.text().strip() or "./var/captures/session.dlhcap",
            },
            "gps": {
                "enabled": self._gps_enabled_checkbox.isChecked(),
                "port": self._gps_port_combo.currentText().strip(),
                "baudrate": self._gps_baudrate_spin.value(),
                "mode": self._gps_mode_combo.currentData(),
                "poll_interval_seconds": self._gps_poll_spin.value(),
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
        palette = self._config_feedback_label.palette()
        role = self._config_feedback_label.foregroundRole()
        color = "#b3261e" if is_error else "#1d4ed8"
        self._config_feedback_label.setText(text)
        self._config_feedback_label.setStyleSheet(f"color: {color};")
        self._config_feedback_label.setPalette(palette)
        self._config_feedback_label.setForegroundRole(role)

    def _refresh(self) -> None:
        snapshot = self._runtime.snapshot()
        self._update_processing_controls()
        self._update_status(snapshot)
        data = self._snapshot_data(snapshot)
        for index, curve in enumerate(self._plots):
            if data is None or data.shape[0] <= index or data.shape[1] == 0:
                curve.setData([])
                continue
            curve.setData(np.arange(data.shape[1]), data[index])
        self._update_analysis(snapshot, data)
        self._update_logs()

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
            "gps_enabled": "已启用" if snapshot.gps_enabled else "未启用",
            "gps_connected": "已连接" if snapshot.gps_connected else "未连接",
            "gps_mode": snapshot.gps_mode,
            "gps_port": snapshot.gps_port or "-",
            "gps_last_timestamp": snapshot.gps_last_timestamp or "-",
            "gps_last_error": snapshot.gps_last_error or "-",
            "gps_fallback_active": "是" if snapshot.gps_fallback_active else "否",
            "last_error": snapshot.last_error or "-",
        }
        for key, label in self._status_labels.items():
            label.setText(values.get(key, "-"))

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

    def _snapshot_data(self, snapshot: RuntimeSnapshot) -> np.ndarray | None:
        if self._data_mode == "raw":
            return slice_for_plot(snapshot.latest_raw, self._settings.gui.max_points_per_trace)
        if self._data_mode == "data1":
            return slice_for_plot(snapshot.latest_data1, self._settings.gui.max_points_per_trace)
        if self._data_mode == "data2":
            return slice_for_plot(snapshot.latest_data2, self._settings.gui.max_points_per_trace)
        return slice_for_plot(snapshot.latest_unwrapped, self._settings.gui.max_points_per_trace)

    def _update_analysis(self, snapshot: RuntimeSnapshot, data: np.ndarray | None) -> None:
        if self._analysis_channel_spin is None or self._analysis_window_spin is None:
            return
        if self._analysis_time_curve is None or self._analysis_psd_curve is None:
            return
        if data is None or data.size == 0:
            self._analysis_time_curve.setData([])
            self._analysis_psd_curve.setData([])
            return

        channel_index = self._analysis_channel_spin.value() - 1
        if channel_index >= data.shape[0]:
            self._analysis_time_curve.setData([])
            self._analysis_psd_curve.setData([])
            return

        channel_data = data[channel_index]
        window = min(self._analysis_window_spin.value(), channel_data.size)
        channel_window = channel_data[-window:]
        self._analysis_time_curve.setData(np.arange(channel_window.size), channel_window)

        sample_rate = self._selected_sample_rate(snapshot)
        freqs, psd = compute_psd(channel_window, sample_rate)
        if freqs.size <= 1:
            self._analysis_psd_curve.setData([])
            return
        self._analysis_psd_curve.setData(freqs[1:], np.maximum(psd[1:], 1e-18))

    def _selected_sample_rate(self, snapshot: RuntimeSnapshot) -> float:
        if self._data_mode == "data1":
            return snapshot.data1_rate
        if self._data_mode == "data2":
            return snapshot.data2_rate
        return snapshot.source_sample_rate or 1.0

    def _update_logs(self) -> None:
        if self._log_view is None or self._log_level_combo is None:
            return
        level = self._log_level_combo.currentText()
        lines = get_recent_logs(300)
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
