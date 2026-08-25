// this file is used only for the development simulation and is not part of the build
import React from 'react';
import { ThemeProvider, StyledEngineProvider } from '@mui/material/styles';
import { Box } from '@mui/material';

import {
    GenericApp,
    I18n,
    type IobTheme,
    Loader,
    type GenericAppProps,
    type GenericAppState,
} from '@iobroker/gui-components';

import DataBrowser from './DataBrowser';
import enLocal from './i18n/en.json';

const styles: Record<string, any> = {
    app: (theme: IobTheme): React.CSSProperties => ({
        backgroundColor: theme.palette.background.default,
        color: theme.palette.text.primary,
        height: '100%',
    }),
};

export default class App extends GenericApp<GenericAppProps, GenericAppState> {
    constructor(props: GenericAppProps) {
        super(props, { ...props });

        this.state = {
            ...this.state,
            theme: this.createTheme(),
        };

        I18n.setTranslations({ en: enLocal });
        I18n.setLanguage((navigator.language || 'en').substring(0, 2).toLowerCase() as ioBroker.Languages);
    }

    render(): React.JSX.Element {
        if (!this.state.loaded) {
            return (
                <StyledEngineProvider injectFirst>
                    <ThemeProvider theme={this.state.theme}>
                        <Loader themeType={this.state.themeType} />
                    </ThemeProvider>
                </StyledEngineProvider>
            );
        }

        return (
            <StyledEngineProvider injectFirst>
                <ThemeProvider theme={this.state.theme}>
                    <Box sx={styles.app}>
                        <DataBrowser
                            oContext={{
                                adapterName: 'sql',
                                socket: this.socket,
                                instance: 0,
                                themeType: this.state.theme.palette.mode,
                                isFloatComma: false,
                                dateFormat: '',
                                forceUpdate: () => {},
                                systemConfig: {} as ioBroker.SystemConfigCommon,
                                theme: this.state.theme,
                                _themeName: this.state.themeName,
                                onCommandRunning: (): void => {},
                            }}
                            alive
                            changed={false}
                            themeName={this.state.theme.palette.mode}
                            common={{} as ioBroker.InstanceCommon}
                            attr="dataBrowser"
                            data={{}}
                            originalData={{}}
                            onError={() => {}}
                            schema={{
                                url: '',
                                i18n: true,
                                name: 'SqlComponentsSet/Components/DataBrowser',
                                type: 'custom',
                            }}
                            onChange={() => {}}
                        />
                    </Box>
                </ThemeProvider>
            </StyledEngineProvider>
        );
    }
}
