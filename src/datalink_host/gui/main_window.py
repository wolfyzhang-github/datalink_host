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


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, runtime: RuntimeService, settings: AppSettings) -> None:
        super().__init__()
        self._runtime = runtime
        self._settings = settings
        self._data_mode = "unwrapped"
        self.setWindowTitle("长基线光纤应变信号监控软件")
        self.resize(1400, 900)
        self._status_labels: dict[str, QtWidgets.QLabel] = {}
        self._plots: list[pg.PlotDataItem] = []
        self._processing_state_label: QtWidgets.QLabel | None = None
        self._start_processing_button: QtWidgets.QPushButton | None = None
        self._pause_processing_button: QtWidgets.QPushButton | None = None
        self._ingest_help_label: QtWidgets.QLabel | None = None
        self._data1_rate_spin: QtWidgets.QDoubleSpinBox | None = None
        self._data2_rate_spin: QtWidgets.QDoubleSpinBox | None = None
        self._data_mode_combo: QtWidgets.QComboBox | None = None
        self._data_host_edit: QtWidgets.QLineEdit | None = None
        self._data_port_spin: QtWidgets.QSpinBox | None = None
        self._data_remote_host_edit: QtWidgets.QLineEdit | None = None
        self._data_remote_port_spin: QtWidgets.QSpinBox | None = None
        self._frame_header_edit: QtWidgets.QLineEdit | None = None
        self._frame_header_size_combo: QtWidgets.QComboBox | None = None
        self._length_field_size_combo: QtWidgets.QComboBox | None = None
        self._length_field_format_combo: QtWidgets.QComboBox | None = None
        self._length_field_units_combo: QtWidgets.QComboBox | None = None
        self._byte_order_combo: QtWidgets.QComboBox | None = None
        self._channel_layout_combo: QtWidgets.QComboBox | None = None
        self._storage_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._datalink_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._storage_root_edit: QtWidgets.QLineEdit | None = None
        self._storage_duration_spin: QtWidgets.QSpinBox | None = None
        self._storage_network_edit: QtWidgets.QLineEdit | None = None
        self._storage_station_edit: QtWidgets.QLineEdit | None = None
        self._storage_location_edit: QtWidgets.QLineEdit | None = None
        self._datalink_host_edit: QtWidgets.QLineEdit | None = None
        self._datalink_port_spin: QtWidgets.QSpinBox | None = None
        self._datalink_ack_checkbox: QtWidgets.QCheckBox | None = None
        self._datalink_send_data2_checkbox: QtWidgets.QCheckBox | None = None
        self._capture_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._capture_path_edit: QtWidgets.QLineEdit | None = None
        self._gps_enabled_checkbox: QtWidgets.QCheckBox | None = None
        self._gps_port_combo: QtWidgets.QComboBox | None = None
        self._gps_baudrate_spin: QtWidgets.QSpinBox | None = None
        self._gps_mode_combo: QtWidgets.QComboBox | None = None
        self._gps_poll_spin: QtWidgets.QDoubleSpinBox | None = None
        self._analysis_channel_spin: QtWidgets.QSpinBox | None = None
        self._analysis_window_spin: QtWidgets.QSpinBox | None = None
        self._analysis_time_curve: pg.PlotDataItem | None = None
        self._analysis_psd_curve: pg.PlotDataItem | None = None
        self._log_view: QtWidgets.QPlainTextEdit | None = None
        self._log_level_combo: QtWidgets.QComboBox | None = None
        self._build_ui()
        self._refresh_gps_ports()

        self._timer = QtCore.QTimer(self)
        self._timer.timeout.connect(self._refresh)
        self._timer.start(settings.gui.refresh_interval_ms)

    def _build_ui(self) -> None:
        central = QtWidgets.QWidget(self)
        root = QtWidgets.QVBoxLayout(central)
        root.addLayout(self._build_status_bar())
        root.addLayout(self._build_controls())
        root.addWidget(self._build_config_panel())
        root.addWidget(self._build_tabs(), stretch=1)
        self.setCentralWidget(central)

    def _build_tabs(self) -> QtWidgets.QWidget:
        tabs = QtWidgets.QTabWidget(self)
        tabs.addTab(self._build_plot_grid(), "总览")
        tabs.addTab(self._build_analysis_tab(), "分析")
        tabs.addTab(self._build_logs_tab(), "日志")
        return tabs

    def _build_status_bar(self) -> QtWidgets.QLayout:
        layout = QtWidgets.QGridLayout()
        fields = [
            ("data_connected", "数据连接"),
            ("control_connected", "控制连接"),
            ("source_sample_rate", "源采样率"),
            ("packets_received", "已收包数"),
            ("bytes_received", "已收字节"),
            ("queue_depth", "队列深度"),
            ("data1_rate", "降采样1 采样率"),
            ("data2_rate", "降采样2 采样率"),
            ("storage_enabled", "本地存储"),
            ("datalink_enabled", "远传开关"),
            ("datalink_connected", "远传连接"),
            ("datalink_packets_sent", "已发包数"),
            ("datalink_bytes_sent", "已发字节"),
            ("datalink_reconnects", "重连次数"),
            ("datalink_last_error", "远传错误"),
            ("capture_enabled", "抓包开关"),
            ("gps_enabled", "GPS 开关"),
            ("gps_connected", "GPS 连接"),
            ("gps_mode", "GPS 模式"),
            ("gps_port", "GPS 串口"),
            ("gps_last_timestamp", "GPS 时间"),
            ("gps_fallback_active", "GPS 回退"),
            ("gps_last_error", "GPS 错误"),
            ("last_error", "最近错误"),
        ]
        for index, (name, label_text) in enumerate(fields):
            label = QtWidgets.QLabel("-")
            self._status_labels[name] = label
            layout.addWidget(QtWidgets.QLabel(label_text), index // 4, (index % 4) * 2)
            layout.addWidget(label, index // 4, (index % 4) * 2 + 1)
        return layout

    def _build_controls(self) -> QtWidgets.QLayout:
        layout = QtWidgets.QVBoxLayout()
        top_row = QtWidgets.QHBoxLayout()
        button_group = QtWidgets.QButtonGroup(self)
        for name, text in (
            ("raw", "原始"),
            ("unwrapped", "相位展开"),
            ("data1", "降采样1"),
            ("data2", "降采样2"),
        ):
            button = QtWidgets.QRadioButton(text)
            if name == self._data_mode:
                button.setChecked(True)
            button.toggled.connect(partial(self._set_mode, name))
            button_group.addButton(button)
            top_row.addWidget(button)
        self._processing_state_label = QtWidgets.QLabel(self)
        self._start_processing_button = QtWidgets.QPushButton("启动数据接收", self)
        self._pause_processing_button = QtWidgets.QPushButton("停止数据接收", self)
        self._start_processing_button.clicked.connect(self._start_processing)
        self._pause_processing_button.clicked.connect(self._pause_processing)
        self._start_processing_button.setMinimumHeight(32)
        self._pause_processing_button.setMinimumHeight(32)
        self._start_processing_button.setStyleSheet("font-weight: 600; padding: 4px 14px;")
        self._pause_processing_button.setStyleSheet("font-weight: 600; padding: 4px 14px;")
        self._start_processing_button.setToolTip("启动 TCP 数据接收；若配置已修改，将按当前配置重新开始接收。")
        self._pause_processing_button.setToolTip("停止 TCP 数据接收，但不会关闭界面，也不会清空已显示的数据。")
        top_row.addSpacing(24)
        top_row.addWidget(self._processing_state_label)
        top_row.addWidget(self._start_processing_button)
        top_row.addWidget(self._pause_processing_button)
        top_row.addStretch(1)
        self._ingest_help_label = QtWidgets.QLabel(
            "上方按钮只控制数据接收是否运行。下方“保存配置”只修改配置：运行中保存会自动重连，停止时保存不会自动启动。",
            self,
        )
        self._ingest_help_label.setWordWrap(True)
        self._ingest_help_label.setStyleSheet("color: #555;")
        layout.addLayout(top_row)
        layout.addWidget(self._ingest_help_label)
        self._update_processing_controls()
        return layout

    def _build_config_panel(self) -> QtWidgets.QWidget:
        widget = QtWidgets.QGroupBox("采集与协议配置", self)
        layout = QtWidgets.QGridLayout(widget)

        self._data1_rate_spin = QtWidgets.QDoubleSpinBox(widget)
        self._data1_rate_spin.setRange(0.1, 10000.0)
        self._data1_rate_spin.setDecimals(2)
        self._data1_rate_spin.setValue(self._settings.processing.data1_rate)

        self._data2_rate_spin = QtWidgets.QDoubleSpinBox(widget)
        self._data2_rate_spin.setRange(0.1, 10000.0)
        self._data2_rate_spin.setDecimals(2)
        self._data2_rate_spin.setValue(self._settings.processing.data2_rate)

        self._data_mode_combo = QtWidgets.QComboBox(widget)
        self._data_mode_combo.addItem("主动连接设备", "client")
        self._data_mode_combo.addItem("监听设备连接", "server")
        self._data_mode_combo.setCurrentIndex(0 if self._settings.data_server.mode == "client" else 1)

        self._data_host_edit = QtWidgets.QLineEdit(self._settings.data_server.host, widget)

        self._data_port_spin = QtWidgets.QSpinBox(widget)
        self._data_port_spin.setRange(1, 65535)
        self._data_port_spin.setValue(self._settings.data_server.port)

        self._data_remote_host_edit = QtWidgets.QLineEdit(self._settings.data_server.remote_host, widget)

        self._data_remote_port_spin = QtWidgets.QSpinBox(widget)
        self._data_remote_port_spin.setRange(1, 65535)
        self._data_remote_port_spin.setValue(self._settings.data_server.remote_port)

        self._frame_header_edit = QtWidgets.QLineEdit(str(self._settings.protocol.frame_header), widget)

        self._frame_header_size_combo = QtWidgets.QComboBox(widget)
        self._frame_header_size_combo.addItems(["2", "4", "8"])
        self._frame_header_size_combo.setCurrentText(str(self._settings.protocol.frame_header_size))

        self._length_field_size_combo = QtWidgets.QComboBox(widget)
        self._length_field_size_combo.addItems(["4", "8"])
        self._length_field_size_combo.setCurrentText(str(self._settings.protocol.length_field_size))

        self._length_field_format_combo = QtWidgets.QComboBox(widget)
        self._length_field_format_combo.addItem("无符号整数", "uint")
        self._length_field_format_combo.addItem("浮点 float64", "float64")
        self._length_field_format_combo.setCurrentIndex(
            0 if self._settings.protocol.length_field_format == "uint" else 1
        )

        self._length_field_units_combo = QtWidgets.QComboBox(widget)
        self._length_field_units_combo.addItem("字节", "bytes")
        self._length_field_units_combo.addItem("数值个数", "values")
        self._length_field_units_combo.setCurrentIndex(
            0 if self._settings.protocol.length_field_units == "bytes" else 1
        )

        self._byte_order_combo = QtWidgets.QComboBox(widget)
        self._byte_order_combo.addItem("小端", "little")
        self._byte_order_combo.addItem("大端", "big")
        self._byte_order_combo.setCurrentIndex(0 if self._settings.protocol.byte_order == "little" else 1)

        self._channel_layout_combo = QtWidgets.QComboBox(widget)
        self._channel_layout_combo.addItem("采样交织", "interleaved")
        self._channel_layout_combo.addItem("按通道连续", "channel-major")
        self._channel_layout_combo.setCurrentIndex(
            0 if self._settings.protocol.channel_layout == "interleaved" else 1
        )

        self._storage_enabled_checkbox = QtWidgets.QCheckBox("启用本地存储", widget)
        self._storage_enabled_checkbox.setChecked(self._settings.storage.enabled)

        self._datalink_enabled_checkbox = QtWidgets.QCheckBox("启用 DataLink 远传", widget)
        self._datalink_enabled_checkbox.setChecked(self._settings.datalink.enabled)

        self._storage_root_edit = QtWidgets.QLineEdit(str(self._settings.storage.root), widget)
        self._storage_duration_spin = QtWidgets.QSpinBox(widget)
        self._storage_duration_spin.setRange(1, 86400)
        self._storage_duration_spin.setValue(self._settings.storage.file_duration_seconds)

        self._storage_network_edit = QtWidgets.QLineEdit(self._settings.storage.network, widget)
        self._storage_station_edit = QtWidgets.QLineEdit(self._settings.storage.station, widget)
        self._storage_location_edit = QtWidgets.QLineEdit(self._settings.storage.location, widget)
        self._datalink_host_edit = QtWidgets.QLineEdit(self._settings.datalink.host, widget)
        self._datalink_port_spin = QtWidgets.QSpinBox(widget)
        self._datalink_port_spin.setRange(1, 65535)
        self._datalink_port_spin.setValue(self._settings.datalink.port)
        self._datalink_ack_checkbox = QtWidgets.QCheckBox("需要 ACK", widget)
        self._datalink_ack_checkbox.setChecked(self._settings.datalink.ack_required)
        self._datalink_send_data2_checkbox = QtWidgets.QCheckBox("发送降采样2", widget)
        self._datalink_send_data2_checkbox.setChecked(self._settings.datalink.send_data2)
        self._capture_enabled_checkbox = QtWidgets.QCheckBox("启用抓包", widget)
        self._capture_enabled_checkbox.setChecked(self._settings.capture.enabled)
        self._capture_path_edit = QtWidgets.QLineEdit(str(self._settings.capture.path), widget)
        self._gps_enabled_checkbox = QtWidgets.QCheckBox("启用 GPS 时间", widget)
        self._gps_enabled_checkbox.setChecked(self._settings.gps.enabled)
        self._gps_port_combo = QtWidgets.QComboBox(widget)
        self._gps_port_combo.setEditable(True)
        self._gps_port_combo.setInsertPolicy(QtWidgets.QComboBox.InsertPolicy.NoInsert)
        self._gps_baudrate_spin = QtWidgets.QSpinBox(widget)
        self._gps_baudrate_spin.setRange(1, 921600)
        self._gps_baudrate_spin.setValue(self._settings.gps.baudrate)
        self._gps_mode_combo = QtWidgets.QComboBox(widget)
        self._gps_mode_combo.addItem("调试模式", "debug")
        self._gps_mode_combo.addItem("部署模式", "deploy")
        self._gps_mode_combo.setCurrentIndex(0 if self._settings.gps.mode == "debug" else 1)
        self._gps_poll_spin = QtWidgets.QDoubleSpinBox(widget)
        self._gps_poll_spin.setRange(0.01, 10.0)
        self._gps_poll_spin.setDecimals(2)
        self._gps_poll_spin.setValue(self._settings.gps.poll_interval_seconds)

        apply_button = QtWidgets.QPushButton("保存配置", widget)
        apply_button.clicked.connect(self._apply_runtime_config)
        apply_button.setToolTip("保存当前配置。若数据接收正在运行，会按新配置自动重连；若已停止，仅保存配置。")
        apply_hint_label = QtWidgets.QLabel(
            "保存规则：运行中保存会自动重建数据连接；已停止时仅保存参数，点击“启动数据接收”后才会生效。",
            widget,
        )
        apply_hint_label.setWordWrap(True)
        apply_hint_label.setStyleSheet("color: #555;")

        browse_button = QtWidgets.QPushButton("浏览...", widget)
        browse_button.clicked.connect(self._choose_storage_root)
        capture_browse_button = QtWidgets.QPushButton("抓包文件...", widget)
        capture_browse_button.clicked.connect(self._choose_capture_path)
        gps_refresh_button = QtWidgets.QPushButton("刷新串口", widget)
        gps_refresh_button.clicked.connect(self._refresh_gps_ports)

        layout.addWidget(QtWidgets.QLabel("降采样1 采样率"), 0, 0)
        layout.addWidget(self._data1_rate_spin, 0, 1)
        layout.addWidget(QtWidgets.QLabel("降采样2 采样率"), 0, 2)
        layout.addWidget(self._data2_rate_spin, 0, 3)
        layout.addWidget(QtWidgets.QLabel("数据接入模式"), 1, 0)
        layout.addWidget(self._data_mode_combo, 1, 1)
        layout.addWidget(QtWidgets.QLabel("本地监听地址"), 1, 2)
        layout.addWidget(self._data_host_edit, 1, 3)
        layout.addWidget(QtWidgets.QLabel("本地监听端口"), 2, 0)
        layout.addWidget(self._data_port_spin, 2, 1)
        layout.addWidget(QtWidgets.QLabel("设备 IP"), 2, 2)
        layout.addWidget(self._data_remote_host_edit, 2, 3)
        layout.addWidget(QtWidgets.QLabel("设备端口"), 3, 0)
        layout.addWidget(self._data_remote_port_spin, 3, 1)
        layout.addWidget(QtWidgets.QLabel("帧头值"), 3, 2)
        layout.addWidget(self._frame_header_edit, 3, 3)
        layout.addWidget(QtWidgets.QLabel("帧头字节数"), 4, 0)
        layout.addWidget(self._frame_header_size_combo, 4, 1)
        layout.addWidget(QtWidgets.QLabel("长度字段字节数"), 4, 2)
        layout.addWidget(self._length_field_size_combo, 4, 3)
        layout.addWidget(QtWidgets.QLabel("长度字段类型"), 5, 0)
        layout.addWidget(self._length_field_format_combo, 5, 1)
        layout.addWidget(QtWidgets.QLabel("长度单位"), 5, 2)
        layout.addWidget(self._length_field_units_combo, 5, 3)
        layout.addWidget(QtWidgets.QLabel("字节序"), 6, 0)
        layout.addWidget(self._byte_order_combo, 6, 1)
        layout.addWidget(QtWidgets.QLabel("通道排列"), 6, 2)
        layout.addWidget(self._channel_layout_combo, 6, 3)
        layout.addWidget(self._storage_enabled_checkbox, 7, 2)
        layout.addWidget(self._datalink_enabled_checkbox, 7, 3)
        layout.addWidget(QtWidgets.QLabel("存储目录"), 8, 0)
        layout.addWidget(self._storage_root_edit, 8, 1, 1, 3)
        layout.addWidget(browse_button, 8, 4)
        layout.addWidget(QtWidgets.QLabel("单文件时长(秒)"), 9, 0)
        layout.addWidget(self._storage_duration_spin, 9, 1)
        layout.addWidget(QtWidgets.QLabel("网络码"), 9, 2)
        layout.addWidget(self._storage_network_edit, 9, 3)
        layout.addWidget(QtWidgets.QLabel("台站码"), 10, 0)
        layout.addWidget(self._storage_station_edit, 10, 1)
        layout.addWidget(QtWidgets.QLabel("位置码"), 10, 2)
        layout.addWidget(self._storage_location_edit, 10, 3)
        layout.addWidget(QtWidgets.QLabel("DataLink 主机"), 11, 0)
        layout.addWidget(self._datalink_host_edit, 11, 1)
        layout.addWidget(QtWidgets.QLabel("DataLink 端口"), 11, 2)
        layout.addWidget(self._datalink_port_spin, 11, 3)
        layout.addWidget(self._datalink_ack_checkbox, 12, 0)
        layout.addWidget(self._datalink_send_data2_checkbox, 12, 1)
        layout.addWidget(self._capture_enabled_checkbox, 12, 2)
        layout.addWidget(self._capture_path_edit, 12, 3)
        layout.addWidget(capture_browse_button, 12, 4)
        layout.addWidget(self._gps_enabled_checkbox, 13, 0)
        layout.addWidget(QtWidgets.QLabel("GPS 串口"), 13, 1)
        layout.addWidget(self._gps_port_combo, 13, 2)
        layout.addWidget(gps_refresh_button, 13, 4)
        layout.addWidget(QtWidgets.QLabel("GPS 波特率"), 14, 0)
        layout.addWidget(self._gps_baudrate_spin, 14, 1)
        layout.addWidget(QtWidgets.QLabel("GPS 模式"), 14, 2)
        layout.addWidget(self._gps_mode_combo, 14, 3)
        layout.addWidget(QtWidgets.QLabel("轮询间隔(秒)"), 15, 0)
        layout.addWidget(self._gps_poll_spin, 15, 1)
        layout.addWidget(apply_hint_label, 16, 0, 1, 4)
        layout.addWidget(apply_button, 16, 4)
        return widget

    def _build_plot_grid(self) -> QtWidgets.QWidget:
        widget = QtWidgets.QWidget(self)
        layout = QtWidgets.QGridLayout(widget)
        colors = ["#6e44ff", "#00916e", "#e4572e", "#f3a712", "#4d9de0", "#c5283d", "#17bebb", "#2e4057"]
        for channel in range(self._settings.protocol.channels):
            plot_widget = pg.PlotWidget(title=f"CH {channel + 1}")
            plot_widget.showGrid(x=True, y=True, alpha=0.2)
            plot_widget.setBackground("w")
            curve = plot_widget.plot(pen=pg.mkPen(colors[channel % len(colors)], width=1.5))
            self._plots.append(curve)
            layout.addWidget(plot_widget, channel // 2, channel % 2)
        return widget

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
        self._analysis_psd_curve = psd_plot.plot(pen=pg.mkPen("#d1495b", width=1.8))
        psd_plot.setLogMode(False, True)

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

    def _refresh_gps_ports(self) -> None:
        if self._gps_port_combo is None:
            return
        current = self._gps_port_combo.currentText().strip()
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

    def _apply_runtime_config(self) -> None:
        assert self._data1_rate_spin is not None
        assert self._data2_rate_spin is not None
        assert self._data_mode_combo is not None
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
        assert self._datalink_enabled_checkbox is not None
        assert self._storage_root_edit is not None
        assert self._storage_duration_spin is not None
        assert self._storage_network_edit is not None
        assert self._storage_station_edit is not None
        assert self._storage_location_edit is not None
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
                "mode": self._data_mode_combo.currentData(),
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
                "root": self._storage_root_edit.text().strip(),
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
            self._update_processing_controls()
        except Exception as exc:  # noqa: BLE001
            QtWidgets.QMessageBox.critical(self, "应用配置失败", str(exc))

    def _start_processing(self) -> None:
        self._runtime.resume_processing()
        self._update_processing_controls()

    def _pause_processing(self) -> None:
        self._runtime.pause_processing()
        self._update_processing_controls()

    def _refresh(self) -> None:
        snapshot = self._runtime.snapshot()
        self._update_processing_controls()
        self._update_status(snapshot)
        data = self._snapshot_data(snapshot)
        for index, curve in enumerate(self._plots):
            if data is None or data.shape[0] <= index or data.shape[1] == 0:
                curve.setData([])
                continue
            x = np.arange(data.shape[1])
            curve.setData(x, data[index])
        self._update_analysis(snapshot, data)
        self._update_logs()

    def _update_status(self, snapshot: RuntimeSnapshot) -> None:
        values = {
            "data_connected": str(snapshot.data_connected),
            "control_connected": str(snapshot.control_connected),
            "source_sample_rate": "-" if snapshot.source_sample_rate is None else f"{snapshot.source_sample_rate:.1f}",
            "packets_received": str(snapshot.packets_received),
            "bytes_received": str(snapshot.bytes_received),
            "queue_depth": str(snapshot.queue_depth),
            "data1_rate": f"{snapshot.data1_rate:.1f}",
            "data2_rate": f"{snapshot.data2_rate:.1f}",
            "storage_enabled": str(snapshot.storage_enabled),
            "datalink_enabled": str(snapshot.datalink_enabled),
            "datalink_connected": str(snapshot.datalink_connected),
            "datalink_packets_sent": str(snapshot.datalink_packets_sent),
            "datalink_bytes_sent": str(snapshot.datalink_bytes_sent),
            "datalink_reconnects": str(snapshot.datalink_reconnects),
            "datalink_last_error": snapshot.datalink_last_error or "-",
            "capture_enabled": str(snapshot.capture_enabled),
            "gps_enabled": str(snapshot.gps_enabled),
            "gps_connected": str(snapshot.gps_connected),
            "gps_mode": snapshot.gps_mode,
            "gps_port": snapshot.gps_port or "-",
            "gps_last_timestamp": snapshot.gps_last_timestamp or "-",
            "gps_fallback_active": str(snapshot.gps_fallback_active),
            "gps_last_error": snapshot.gps_last_error or "-",
            "last_error": snapshot.last_error or "-",
        }
        for key, label in self._status_labels.items():
            label.setText(values[key])

    def _update_processing_controls(self) -> None:
        if (
            self._processing_state_label is None
            or self._start_processing_button is None
            or self._pause_processing_button is None
        ):
            return
        active = self._runtime.is_processing_active()
        self._processing_state_label.setText("数据接收状态: 运行中" if active else "数据接收状态: 已停止")
        self._start_processing_button.setEnabled(not active)
        self._pause_processing_button.setEnabled(active)

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
