export interface AppEvents {
    'region:selected': (region: string) => void;
    'region:unselected': (region: string) => void;
    'time:selected': (time: Date) => void;
    'time:unselected': (time: Date) => void;
}

