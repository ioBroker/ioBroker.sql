import React from 'react';

import {
    Alert,
    Box,
    Button,
    Checkbox,
    Dialog,
    DialogActions,
    DialogContent,
    DialogTitle,
    IconButton,
    InputAdornment,
    LinearProgress,
    List,
    ListItemButton,
    ListItemText,
    MenuItem,
    Paper,
    Select,
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableRow,
    TextField,
    Tooltip,
    Typography,
    type SxProps,
    type Theme,
} from '@mui/material';
import {
    Add,
    ArrowDownward,
    ArrowUpward,
    ChevronLeft,
    ChevronRight,
    Close,
    Delete,
    Edit,
    Refresh,
    Search,
} from '@mui/icons-material';

// important to import from the package and not from some children
import { ConfigGeneric, type ConfigGenericProps, type ConfigGenericState } from '@iobroker/json-config';
import { I18n, Utils } from '@iobroker/gui-components';

type StorageType = 'Number' | 'String' | 'Boolean';

interface DataPoint {
    id: string;
    index: number;
    type: StorageType | null;
}

interface RawEntry {
    ts: number;
    val: number | string | boolean | null;
    ack?: number | boolean | null;
    q?: number | null;
    from?: string | null;
}

interface EntryDialog {
    /** the entry that is edited, or null for a new entry */
    original: RawEntry | null;
    ts: string;
    val: string;
    ack: boolean;
    q: string;
}

interface DataBrowserState extends ConfigGenericState {
    points: DataPoint[] | null;
    /** name of the object behind a datapoint, if it has one and it differs from the ID */
    names: Record<string, string>;
    filter: string;
    selected: DataPoint | null;
    rows: RawEntry[];
    total: number;
    page: number;
    rowsPerPage: number;
    sort: 'asc' | 'desc';
    start: string;
    end: string;
    checked: number[];
    loadingPoints: boolean;
    loadingRows: boolean;
    errorText: string;
    entryDialog: EntryDialog | null;
    confirmText: string;
    confirmAction: (() => void) | null;
}

const styles: Record<string, React.CSSProperties> = {
    root: {
        display: 'flex',
        width: '100%',
        height: 'calc(100vh - 220px)',
        minHeight: 400,
        gap: 8,
    },
    list: {
        width: 320,
        minWidth: 220,
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
    },
    filterField: {
        padding: 8,
        flexShrink: 0,
    },
    listItems: {
        overflowY: 'auto',
        flexGrow: 1,
        // without it the list refuses to shrink below its content and squeezes the filter field
        minHeight: 0,
    },
    data: {
        flexGrow: 1,
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
    },
    toolbar: {
        display: 'flex',
        alignItems: 'center',
        flexWrap: 'nowrap',
        gap: 8,
        padding: 8,
        overflow: 'hidden',
        // a long table must never squeeze the toolbar - it keeps its natural height
        flexShrink: 0,
    },
    // the ID is the only element that may shrink, so the controls always stay on one line
    title: {
        display: 'flex',
        flexDirection: 'column',
        flexGrow: 1,
        minWidth: 60,
        overflow: 'hidden',
    },
    titleId: {
        fontWeight: 'bold',
        whiteSpace: 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
    },
    titleName: {
        fontSize: 'smaller',
        opacity: 0.7,
        whiteSpace: 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
    },
    dateField: {
        width: 200,
        flexShrink: 0,
    },
    table: {
        overflowY: 'auto',
        flexGrow: 1,
        // a flex item does not shrink below its content by default, so the table would push the toolbar away
        minHeight: 0,
    },
    nowrap: {
        whiteSpace: 'nowrap',
    },
    pageInfo: {
        whiteSpace: 'nowrap',
        flexShrink: 0,
    },
    grow: {
        flexGrow: 1,
    },
};

/**
 * Chrome draws the icon of the native date/time picker always dark, so it disappears on a dark theme.
 * `filter: invert` flips it to white there and leaves it untouched on a light theme.
 */
const pickerIconSx: SxProps<Theme> = theme => ({
    '& input::-webkit-calendar-picker-indicator': {
        filter: theme.palette.mode === 'dark' ? 'invert(1)' : 'none',
        cursor: 'pointer',
    },
});

function formatTs(ts: number): string {
    const date = new Date(ts);
    if (isNaN(date.getTime())) {
        return String(ts);
    }
    const pad = (value: number, size?: number): string => value.toString().padStart(size || 2, '0');
    return (
        `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
        `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)}`
    );
}

/** local date of a timestamp as `YYYY-MM-DD` for an input field */
function toDateInput(date: Date): string {
    const pad = (value: number): string => value.toString().padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

/** local time of a timestamp as `HH:mm:ss` for an input field */
function toTimeInput(date: Date): string {
    const pad = (value: number): string => value.toString().padStart(2, '0');
    return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function fromInputValue(value: string): number | undefined {
    if (!value) {
        return undefined;
    }
    const ts = new Date(value).getTime();
    return isNaN(ts) ? undefined : ts;
}

export default class DataBrowser extends ConfigGeneric<ConfigGenericProps, DataBrowserState> {
    constructor(props: ConfigGenericProps) {
        super(props);

        this.state = {
            ...this.state,
            points: null,
            names: {},
            filter: '',
            selected: null,
            rows: [],
            total: 0,
            page: 0,
            rowsPerPage: 100,
            sort: 'desc',
            start: '',
            end: '',
            checked: [],
            loadingPoints: false,
            loadingRows: false,
            errorText: '',
            entryDialog: null,
            confirmText: '',
            confirmAction: null,
        };
    }

    async componentDidMount(): Promise<void> {
        await super.componentDidMount();
        if (this.props.alive) {
            await this.loadDatapoints();
        }
    }

    async componentDidUpdate(prevProps: ConfigGenericProps): Promise<void> {
        // the instance was started in the meantime
        if (this.props.alive && !prevProps.alive && !this.state.points) {
            await this.loadDatapoints();
        }
    }

    get instance(): string {
        return `${this.props.oContext.adapterName}.${this.props.oContext.instance}`;
    }

    async sendToInstance(command: string, data: Record<string, any> | Record<string, any>[]): Promise<any> {
        const result = await this.props.oContext.socket.sendTo(this.instance, command, data);
        if (result?.error) {
            throw new Error(result.error);
        }
        return result;
    }

    async loadDatapoints(): Promise<void> {
        this.setState({ loadingPoints: true, errorText: '' });
        try {
            const result = await this.sendToInstance('getDatapoints', {});
            const points: DataPoint[] = result?.result || [];
            this.setState({ points, loadingPoints: false }, () => void this.loadNames(points));
        } catch (e: any) {
            this.setState({ loadingPoints: false, points: [], errorText: e.message });
        }
    }

    /**
     * Read the names of the objects behind the datapoints.
     *
     * They are pure comfort: the database only knows IDs, and a datapoint may have no object at all
     * anymore. So a failure here must not disturb the list.
     */
    async loadNames(points: DataPoint[]): Promise<void> {
        if (!points.length) {
            return;
        }
        try {
            const objects = await this.props.oContext.socket.getObjectsById(points.map(point => point.id));
            const names: Record<string, string> = {};
            for (const point of points) {
                const obj = objects?.[point.id];
                const name = obj ? Utils.getObjectNameFromObj(obj, null, { language: I18n.getLanguage() }) : '';
                if (name && name !== point.id) {
                    names[point.id] = name;
                }
            }
            this.setState({ names });
        } catch (e: any) {
            console.warn(`Cannot read the names of the objects: ${e.message}`);
        }
    }

    async loadRows(): Promise<void> {
        const point = this.state.selected;
        if (!point) {
            return;
        }
        this.setState({ loadingRows: true, errorText: '' });
        try {
            const result = await this.sendToInstance('getRawEntries', {
                id: point.id,
                start: fromInputValue(this.state.start),
                end: fromInputValue(this.state.end),
                limit: this.state.rowsPerPage,
                offset: this.state.page * this.state.rowsPerPage,
                sort: this.state.sort,
            });
            this.setState({
                rows: result?.result || [],
                total: result?.total || 0,
                checked: [],
                loadingRows: false,
            });
        } catch (e: any) {
            this.setState({ rows: [], total: 0, loadingRows: false, errorText: e.message });
        }
    }

    selectDatapoint(point: DataPoint): void {
        this.setState({ selected: point, page: 0, rows: [], total: 0, checked: [] }, () => void this.loadRows());
    }

    /** Convert the entered value into the type of the datapoint */
    parseValue(value: string): number | string | boolean | null {
        if (value === '') {
            return null;
        }
        if (this.state.selected?.type === 'Number') {
            const parsed = parseFloat(value.replace(',', '.'));
            return isNaN(parsed) ? 0 : parsed;
        }
        if (this.state.selected?.type === 'Boolean') {
            return value === 'true' || value === '1';
        }
        return value;
    }

    async saveEntry(): Promise<void> {
        const dialog = this.state.entryDialog;
        const point = this.state.selected;
        if (!dialog || !point) {
            return;
        }
        const ts = parseInt(dialog.ts, 10);
        if (!ts) {
            this.setState({ errorText: 'Invalid timestamp' });
            return;
        }
        const state = {
            ts,
            val: this.parseValue(dialog.val),
            ack: dialog.ack,
            q: parseInt(dialog.q, 10) || 0,
        };

        this.setState({ entryDialog: null, loadingRows: true });
        try {
            if (dialog.original) {
                await this.sendToInstance('update', { id: point.id, state });
            } else {
                await this.sendToInstance('storeState', { id: point.id, state });
            }
        } catch (e: any) {
            this.setState({ errorText: e.message });
        }
        await this.loadRows();
    }

    async deleteEntries(entries: number[]): Promise<void> {
        const point = this.state.selected;
        if (!point || !entries.length) {
            return;
        }
        this.setState({ loadingRows: true });
        try {
            if (entries.length === 1) {
                // the single call reports errors back, the array call does not
                await this.sendToInstance('delete', { id: point.id, ts: entries[0] });
            } else {
                await this.sendToInstance(
                    'delete',
                    entries.map(ts => ({ id: point.id, ts })),
                );
            }
        } catch (e: any) {
            this.setState({ errorText: e.message });
        }
        await this.loadRows();
    }

    renderDatapoints(): React.JSX.Element {
        const filter = this.state.filter.toLowerCase();
        const points = (this.state.points || []).filter(
            point =>
                !filter ||
                point.id.toLowerCase().includes(filter) ||
                this.state.names[point.id]?.toLowerCase().includes(filter),
        );

        return (
            <Paper style={styles.list}>
                <TextField
                    variant="standard"
                    style={styles.filterField}
                    value={this.state.filter}
                    onChange={e => this.setState({ filter: e.target.value })}
                    placeholder={I18n.t('sql_filter')}
                    slotProps={{
                        input: {
                            startAdornment: (
                                <InputAdornment position="start">
                                    <Search />
                                </InputAdornment>
                            ),
                            endAdornment: this.state.filter ? (
                                <InputAdornment position="end">
                                    <IconButton
                                        size="small"
                                        onClick={() => this.setState({ filter: '' })}
                                    >
                                        <Close />
                                    </IconButton>
                                </InputAdornment>
                            ) : null,
                        },
                    }}
                />
                {this.state.loadingPoints ? <LinearProgress /> : null}
                <List
                    dense
                    style={styles.listItems}
                >
                    {points.map(point => (
                        <ListItemButton
                            key={point.id}
                            selected={this.state.selected?.id === point.id}
                            onClick={() => this.selectDatapoint(point)}
                            sx={theme => ({
                                '&.Mui-selected': {
                                    backgroundColor: theme.palette.primary.main,
                                    color: theme.palette.primary.contrastText,
                                    '& .MuiListItemText-secondary': {
                                        color: theme.palette.primary.contrastText,
                                        opacity: 0.8,
                                    },
                                    '&:hover': {
                                        backgroundColor: theme.palette.primary.dark,
                                    },
                                },
                            })}
                        >
                            <ListItemText
                                primary={point.id}
                                secondary={
                                    <>
                                        {this.state.names[point.id] ? <b>{this.state.names[point.id]}</b> : null}
                                        {this.state.names[point.id] ? ' ' : ''}
                                        {`[${point.type || '?'}]`}
                                    </>
                                }
                                slotProps={{
                                    primary: { style: { wordBreak: 'break-all' } },
                                    secondary: { style: { wordBreak: 'break-word' } },
                                }}
                            />
                        </ListItemButton>
                    ))}
                    {!points.length && !this.state.loadingPoints ? (
                        <Typography style={{ padding: 16, opacity: 0.7 }}>
                            {I18n.t(this.state.filter ? 'sql_no_data' : 'sql_no_datapoints')}
                        </Typography>
                    ) : null}
                </List>
            </Paper>
        );
    }

    renderToolbar(): React.JSX.Element {
        const { page, rowsPerPage, total } = this.state;
        const first = total ? page * rowsPerPage + 1 : 0;
        const last = Math.min((page + 1) * rowsPerPage, total);

        return (
            <div style={styles.toolbar}>
                <div style={styles.title}>
                    <Tooltip title={this.state.selected?.id || ''}>
                        <span style={styles.titleId}>{this.state.selected?.id}</span>
                    </Tooltip>
                    {this.state.selected && this.state.names[this.state.selected.id] ? (
                        <span style={styles.titleName}>{this.state.names[this.state.selected.id]}</span>
                    ) : null}
                </div>
                <Tooltip title={I18n.t('sql_range_start')}>
                    <TextField
                        variant="outlined"
                        size="small"
                        style={styles.dateField}
                        sx={pickerIconSx}
                        type="datetime-local"
                        value={this.state.start}
                        onChange={e => this.setState({ start: e.target.value, page: 0 }, () => void this.loadRows())}
                    />
                </Tooltip>
                <Tooltip title={I18n.t('sql_range_end')}>
                    <TextField
                        variant="outlined"
                        size="small"
                        style={styles.dateField}
                        sx={pickerIconSx}
                        type="datetime-local"
                        value={this.state.end}
                        onChange={e => this.setState({ end: e.target.value, page: 0 }, () => void this.loadRows())}
                    />
                </Tooltip>
                <Select
                    variant="outlined"
                    size="small"
                    value={rowsPerPage}
                    onChange={e =>
                        this.setState({ rowsPerPage: Number(e.target.value), page: 0 }, () => void this.loadRows())
                    }
                >
                    {[50, 100, 500, 1000].map(count => (
                        <MenuItem
                            key={count}
                            value={count}
                        >
                            {count}
                        </MenuItem>
                    ))}
                </Select>
                <Tooltip title={I18n.t(this.state.sort === 'desc' ? 'sql_sort_desc' : 'sql_sort_asc')}>
                    <IconButton
                        size="small"
                        onClick={() =>
                            this.setState(
                                { sort: this.state.sort === 'desc' ? 'asc' : 'desc', page: 0 },
                                () => void this.loadRows(),
                            )
                        }
                    >
                        {this.state.sort === 'desc' ? <ArrowDownward /> : <ArrowUpward />}
                    </IconButton>
                </Tooltip>
                <Typography style={styles.pageInfo}>
                    {I18n.t('sql_page', first.toString(), last.toString(), total.toString())}
                </Typography>
                <IconButton
                    size="small"
                    disabled={!page}
                    onClick={() => this.setState({ page: page - 1 }, () => void this.loadRows())}
                >
                    <ChevronLeft />
                </IconButton>
                <IconButton
                    size="small"
                    disabled={last >= total}
                    onClick={() => this.setState({ page: page + 1 }, () => void this.loadRows())}
                >
                    <ChevronRight />
                </IconButton>
                <Tooltip title={I18n.t('sql_refresh')}>
                    <IconButton
                        size="small"
                        onClick={() => void this.loadRows()}
                    >
                        <Refresh />
                    </IconButton>
                </Tooltip>
                <Tooltip title={I18n.t('sql_add')}>
                    <IconButton
                        size="small"
                        color="primary"
                        onClick={() =>
                            this.setState({
                                entryDialog: {
                                    original: null,
                                    ts: Date.now().toString(),
                                    val: '',
                                    ack: true,
                                    q: '0',
                                },
                            })
                        }
                    >
                        <Add />
                    </IconButton>
                </Tooltip>
                <Tooltip title={I18n.t('sql_delete_selected')}>
                    <span>
                        <IconButton
                            size="small"
                            disabled={!this.state.checked.length}
                            onClick={() =>
                                this.setState({
                                    confirmText: I18n.t('sql_confirm_delete', this.state.checked.length.toString()),
                                    confirmAction: () => void this.deleteEntries(this.state.checked),
                                })
                            }
                        >
                            <Delete />
                        </IconButton>
                    </span>
                </Tooltip>
            </div>
        );
    }

    renderTable(): React.JSX.Element {
        return (
            <div style={styles.table}>
                <Table
                    size="small"
                    stickyHeader
                >
                    <TableHead>
                        <TableRow>
                            <TableCell padding="checkbox">
                                <Checkbox
                                    indeterminate={
                                        !!this.state.checked.length &&
                                        this.state.checked.length < this.state.rows.length
                                    }
                                    checked={
                                        !!this.state.rows.length && this.state.checked.length === this.state.rows.length
                                    }
                                    onChange={e =>
                                        this.setState({
                                            checked: e.target.checked ? this.state.rows.map(row => row.ts) : [],
                                        })
                                    }
                                />
                            </TableCell>
                            <TableCell>{I18n.t('sql_time')}</TableCell>
                            <TableCell>{I18n.t('sql_value')}</TableCell>
                            <TableCell>{I18n.t('sql_ack')}</TableCell>
                            <TableCell>{I18n.t('sql_quality')}</TableCell>
                            <TableCell>{I18n.t('sql_from')}</TableCell>
                            <TableCell />
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {this.state.rows.map(row => (
                            <TableRow
                                key={row.ts}
                                hover
                            >
                                <TableCell padding="checkbox">
                                    <Checkbox
                                        checked={this.state.checked.includes(row.ts)}
                                        onChange={e =>
                                            this.setState({
                                                checked: e.target.checked
                                                    ? [...this.state.checked, row.ts]
                                                    : this.state.checked.filter(ts => ts !== row.ts),
                                            })
                                        }
                                    />
                                </TableCell>
                                <TableCell style={styles.nowrap}>
                                    <Tooltip title={row.ts.toString()}>
                                        <span>{formatTs(row.ts)}</span>
                                    </Tooltip>
                                </TableCell>
                                <TableCell style={{ wordBreak: 'break-all' }}>
                                    {row.val === null || row.val === undefined ? (
                                        <span style={{ opacity: 0.5 }}>null</span>
                                    ) : (
                                        row.val.toString()
                                    )}
                                </TableCell>
                                <TableCell>{row.ack ? '✓' : ''}</TableCell>
                                <TableCell>{row.q ?? ''}</TableCell>
                                <TableCell style={{ opacity: 0.7 }}>{row.from || ''}</TableCell>
                                <TableCell padding="none">
                                    <IconButton
                                        size="small"
                                        onClick={() =>
                                            this.setState({
                                                entryDialog: {
                                                    original: row,
                                                    ts: row.ts.toString(),
                                                    val:
                                                        row.val === null || row.val === undefined
                                                            ? ''
                                                            : row.val.toString(),
                                                    ack: !!row.ack,
                                                    q: (row.q ?? 0).toString(),
                                                },
                                            })
                                        }
                                    >
                                        <Edit fontSize="small" />
                                    </IconButton>
                                    <IconButton
                                        size="small"
                                        onClick={() =>
                                            this.setState({
                                                confirmText: I18n.t('sql_confirm_delete_one', formatTs(row.ts)),
                                                confirmAction: () => void this.deleteEntries([row.ts]),
                                            })
                                        }
                                    >
                                        <Delete fontSize="small" />
                                    </IconButton>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
                {!this.state.rows.length && !this.state.loadingRows ? (
                    <Typography style={{ padding: 16, opacity: 0.7 }}>{I18n.t('sql_no_data')}</Typography>
                ) : null}
            </div>
        );
    }

    /**
     * Date, time and milliseconds of one entry.
     *
     * The timestamp is the primary key of the entry, so it can only be set for a new one. `dialog.ts` stays
     * the leading value in milliseconds, the three fields only show and change parts of it.
     */
    renderTimeSelector(dialog: EntryDialog): React.JSX.Element {
        const ts = parseInt(dialog.ts, 10);
        const valid = !isNaN(ts);
        const disabled = !!dialog.original;

        const changePart = (part: 'date' | 'time' | 'ms', value: string): void => {
            const date = new Date(valid ? ts : Date.now());
            let dateValue = toDateInput(date);
            let timeValue = toTimeInput(date);
            let ms = date.getMilliseconds();

            if (part === 'date') {
                if (!value) {
                    return;
                }
                dateValue = value;
            } else if (part === 'time') {
                if (!value) {
                    return;
                }
                // the time input delivers HH:mm without seconds if they are zero
                timeValue = value.length === 5 ? `${value}:00` : value;
            } else {
                ms = Math.min(999, Math.max(0, parseInt(value, 10) || 0));
            }

            // a date-time without time zone is parsed as local time - exactly what the fields show
            const newTs = new Date(`${dateValue}T${timeValue}.${ms.toString().padStart(3, '0')}`).getTime();
            if (!isNaN(newTs)) {
                this.setState({ entryDialog: { ...dialog, ts: newTs.toString() } });
            }
        };

        return (
            <div>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                    <TextField
                        label={I18n.t('sql_date')}
                        type="date"
                        value={valid ? toDateInput(new Date(ts)) : ''}
                        disabled={disabled}
                        style={{ flexGrow: 1 }}
                        sx={pickerIconSx}
                        slotProps={{ inputLabel: { shrink: true } }}
                        onChange={e => changePart('date', e.target.value)}
                    />
                    <TextField
                        label={I18n.t('sql_time')}
                        type="time"
                        value={valid ? toTimeInput(new Date(ts)) : ''}
                        disabled={disabled}
                        style={{ flexGrow: 1 }}
                        sx={pickerIconSx}
                        slotProps={{ inputLabel: { shrink: true }, htmlInput: { step: 1 } }}
                        onChange={e => changePart('time', e.target.value)}
                    />
                    <TextField
                        label={I18n.t('sql_ms')}
                        type="number"
                        value={valid ? new Date(ts).getMilliseconds() : ''}
                        disabled={disabled}
                        style={{ width: 100 }}
                        slotProps={{ inputLabel: { shrink: true }, htmlInput: { min: 0, max: 999 } }}
                        onChange={e => changePart('ms', e.target.value)}
                    />
                </div>
                <Typography
                    variant="caption"
                    style={{ opacity: 0.7 }}
                >
                    {valid ? `${formatTs(ts)} · ${ts}` : ''}
                </Typography>
            </div>
        );
    }

    renderEntryDialog(): React.JSX.Element | null {
        const dialog = this.state.entryDialog;
        if (!dialog) {
            return null;
        }

        return (
            <Dialog
                open={!0}
                maxWidth="sm"
                fullWidth
                onClose={() => this.setState({ entryDialog: null })}
            >
                <DialogTitle>{I18n.t(dialog.original ? 'sql_save' : 'sql_add')}</DialogTitle>
                <DialogContent style={{ display: 'flex', flexDirection: 'column', gap: 16, paddingTop: 8 }}>
                    {this.renderTimeSelector(dialog)}
                    {this.state.selected?.type === 'Boolean' ? (
                        <Select
                            value={dialog.val === 'true' || dialog.val === '1' ? 'true' : 'false'}
                            onChange={e => this.setState({ entryDialog: { ...dialog, val: e.target.value } })}
                        >
                            <MenuItem value="true">true</MenuItem>
                            <MenuItem value="false">false</MenuItem>
                        </Select>
                    ) : (
                        <TextField
                            label={I18n.t('sql_value')}
                            value={dialog.val}
                            autoFocus
                            onChange={e => this.setState({ entryDialog: { ...dialog, val: e.target.value } })}
                        />
                    )}
                    <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
                        <Checkbox
                            checked={dialog.ack}
                            onChange={e => this.setState({ entryDialog: { ...dialog, ack: e.target.checked } })}
                        />
                        <Typography>{I18n.t('sql_ack')}</Typography>
                        <TextField
                            label={I18n.t('sql_quality')}
                            value={dialog.q}
                            type="number"
                            onChange={e => this.setState({ entryDialog: { ...dialog, q: e.target.value } })}
                        />
                    </div>
                </DialogContent>
                <DialogActions>
                    <Button
                        variant="contained"
                        color="primary"
                        onClick={() => void this.saveEntry()}
                    >
                        {I18n.t('sql_save')}
                    </Button>
                    <Button
                        variant="outlined"
                        onClick={() => this.setState({ entryDialog: null })}
                    >
                        {I18n.t('sql_cancel')}
                    </Button>
                </DialogActions>
            </Dialog>
        );
    }

    renderConfirmDialog(): React.JSX.Element | null {
        if (!this.state.confirmAction) {
            return null;
        }

        return (
            <Dialog
                open={!0}
                onClose={() => this.setState({ confirmText: '', confirmAction: null })}
            >
                <DialogTitle>{I18n.t('sql_delete')}</DialogTitle>
                <DialogContent>{this.state.confirmText}</DialogContent>
                <DialogActions>
                    <Button
                        variant="contained"
                        color="error"
                        startIcon={<Delete />}
                        onClick={() => {
                            const action = this.state.confirmAction;
                            this.setState({ confirmText: '', confirmAction: null }, () => action?.());
                        }}
                    >
                        {I18n.t('sql_delete')}
                    </Button>
                    <Button
                        variant="outlined"
                        onClick={() => this.setState({ confirmText: '', confirmAction: null })}
                    >
                        {I18n.t('sql_cancel')}
                    </Button>
                </DialogActions>
            </Dialog>
        );
    }

    renderItem(): React.JSX.Element {
        if (!this.props.alive) {
            return <Alert severity="info">{I18n.t('sql_instance_not_alive')}</Alert>;
        }

        return (
            <Box style={styles.root}>
                {this.renderDatapoints()}
                <Paper style={styles.data}>
                    {this.state.errorText ? (
                        <Alert
                            severity="error"
                            style={{ flexShrink: 0 }}
                            onClose={() => this.setState({ errorText: '' })}
                        >
                            {this.state.errorText}
                        </Alert>
                    ) : null}
                    {this.state.selected ? (
                        <>
                            {this.renderToolbar()}
                            {this.state.loadingRows ? <LinearProgress /> : null}
                            {this.renderTable()}
                        </>
                    ) : (
                        <Typography style={{ padding: 16, opacity: 0.7 }}>{I18n.t('sql_select_datapoint')}</Typography>
                    )}
                </Paper>
                {this.renderEntryDialog()}
                {this.renderConfirmDialog()}
            </Box>
        );
    }
}
