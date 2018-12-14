use crate::keys::describe_key;
use crate::tree_menu::TreeMenu;
use crate::{Canvas, Color, GfxCtx, Text, TEXT_FG_COLOR};
use geom::Pt2D;
use piston::input::{
    Button, Event, IdleArgs, Key, MouseButton, MouseCursorEvent, MouseScrollEvent, PressEvent,
    ReleaseEvent, UpdateEvent,
};
use std::collections::{BTreeMap, HashMap};

// As we check for user input, record the input and the thing that would happen. This will let us
// build up some kind of OSD of possible actions.
pub struct UserInput {
    event: Event,
    event_consumed: bool,
    unimportant_actions: Vec<String>,
    important_actions: Vec<String>,

    pub(crate) context_menu: Option<ContextMenu>,

    // If two different callers both expect the same key, there's likely an unintentional conflict.
    reserved_keys: HashMap<Key, String>,

    // TODO hack :(
    empty_event: Event,

    unimportant_actions_tree: TreeMenu,
}

impl UserInput {
    pub(crate) fn new(
        event: Event,
        context_menu: Option<ContextMenu>,
        canvas: &Canvas,
    ) -> UserInput {
        let mut input = UserInput {
            event,
            event_consumed: false,
            unimportant_actions: Vec::new(),
            important_actions: Vec::new(),
            context_menu,
            reserved_keys: HashMap::new(),
            empty_event: Event::from(IdleArgs { dt: 0.0 }),
            unimportant_actions_tree: TreeMenu::new(),
        };

        // Create the context menu here, even if one already existed.
        if input.button_pressed(MouseButton::Right) {
            input.context_menu = Some(ContextMenu {
                actions: BTreeMap::new(),
                origin: canvas.get_cursor_in_map_space(),
            });
        }
        // TODO Or left clicking outside of the menu
        // TODO If the user left clicks on a menu item, then mark that action as selected, and
        // ensure contextual_action is called this round.
        // TODO If the user hovers on a menu item, mark it for later highlighting.
        if input.context_menu.is_some() && input.key_pressed(Key::Escape, "cancel the context menu")
        {
            input.context_menu = None;
        }

        input
    }

    pub fn number_chosen(&mut self, num_options: usize, action: &str) -> Option<usize> {
        assert!(num_options >= 1 && num_options <= 9);

        // TODO less repetition, an array of keys probably
        if num_options >= 1 {
            self.reserve_key(Key::D1, action);
        }
        if num_options >= 2 {
            self.reserve_key(Key::D2, action);
        }
        if num_options >= 3 {
            self.reserve_key(Key::D3, action);
        }
        if num_options >= 4 {
            self.reserve_key(Key::D4, action);
        }
        if num_options >= 5 {
            self.reserve_key(Key::D5, action);
        }
        if num_options >= 6 {
            self.reserve_key(Key::D6, action);
        }
        if num_options >= 7 {
            self.reserve_key(Key::D7, action);
        }
        if num_options >= 8 {
            self.reserve_key(Key::D8, action);
        }
        if num_options >= 9 {
            self.reserve_key(Key::D9, action);
        }

        if self.event_consumed {
            return None;
        }

        let num = if let Some(Button::Keyboard(key)) = self.event.press_args() {
            match key {
                Key::D1 => Some(1),
                Key::D2 => Some(2),
                Key::D3 => Some(3),
                Key::D4 => Some(4),
                Key::D5 => Some(5),
                Key::D6 => Some(6),
                Key::D7 => Some(7),
                Key::D8 => Some(8),
                Key::D9 => Some(9),
                _ => None,
            }
        } else {
            None
        };
        match num {
            Some(n) if n <= num_options => {
                self.consume_event();
                Some(n)
            }
            _ => {
                self.important_actions.push(String::from(action));
                None
            }
        }
    }

    pub fn key_pressed(&mut self, key: Key, action: &str) -> bool {
        self.reserve_key(key, action);

        if self.event_consumed {
            return false;
        }

        if let Some(Button::Keyboard(pressed)) = self.event.press_args() {
            if key == pressed {
                self.consume_event();
                return true;
            }
        }
        self.important_actions
            .push(format!("Press {} to {}", describe_key(key), action));
        false
    }

    pub fn contextual_action(&mut self, hotkey: Key, action: &str) -> bool {
        if let Some(ref mut menu) = self.context_menu.as_mut() {
            // We could be initially populating the menu because the user just right-clicked, or
            // this could be a later round.
            if let Some(prev_action) = menu.actions.get(&hotkey) {
                if prev_action != action {
                    panic!(
                        "Context menu uses hotkey {:?} for both {} and {}",
                        hotkey, prev_action, action
                    );
                }
            } else {
                menu.actions.insert(hotkey, action.to_string());
            }

            if self.event_consumed {
                return false;
            }

            if let Some(Button::Keyboard(pressed)) = self.event.press_args() {
                if hotkey == pressed {
                    self.consume_event();
                    self.context_menu = None;
                    return true;
                }
            }
            false
        } else {
            // If the menu's not active (the user hasn't right-clicked yet), then still allow the
            // legacy behavior of just pressing the hotkey.
            self.key_pressed(hotkey, &format!("CONTEXTUAL: {}", action))
        }
    }

    pub fn unimportant_key_pressed(&mut self, key: Key, category: &str, action: &str) -> bool {
        self.reserve_key(key, action);

        if self.event_consumed {
            return false;
        }

        if let Some(Button::Keyboard(pressed)) = self.event.press_args() {
            if key == pressed {
                self.consume_event();
                return true;
            }
        }
        self.unimportant_actions
            .push(format!("Press {} to {}", describe_key(key), action));
        self.unimportant_actions_tree
            .add_action(Some(key), category, action);
        false
    }

    pub fn key_released(&mut self, key: Key) -> bool {
        if self.event_consumed {
            return false;
        }

        if let Some(Button::Keyboard(released)) = self.event.release_args() {
            if key == released {
                self.consume_event();
                return true;
            }
        }
        false
    }

    // No consuming for these?
    pub(crate) fn button_pressed(&mut self, btn: MouseButton) -> bool {
        if let Some(Button::Mouse(pressed)) = self.event.press_args() {
            btn == pressed
        } else {
            false
        }
    }

    pub(crate) fn button_released(&mut self, btn: MouseButton) -> bool {
        if let Some(Button::Mouse(released)) = self.event.release_args() {
            btn == released
        } else {
            false
        }
    }

    pub fn get_moved_mouse(&self) -> Option<(f64, f64)> {
        self.event
            .mouse_cursor_args()
            .map(|pair| (pair[0], pair[1]))
    }

    pub(crate) fn get_mouse_scroll(&self) -> Option<(f64, f64)> {
        self.event
            .mouse_scroll_args()
            .map(|pair| (pair[0], pair[1]))
    }

    pub fn is_update_event(&mut self) -> bool {
        if self.event_consumed {
            return false;
        }

        if self.event.update_args().is_some() {
            self.consume_event();
            return true;
        }

        false
    }

    // The point of hiding this is to make it easy to migrate between Piston and gfx+winit, but
    // within this crate, everything has to be adjusted anyway.
    pub(crate) fn use_event_directly(&mut self) -> &Event {
        if self.event_consumed {
            return &self.empty_event;
        }
        self.consume_event();
        &self.event
    }

    fn consume_event(&mut self) {
        assert!(!self.event_consumed);
        self.event_consumed = true;
    }

    // TODO Not sure this is a good idea
    pub fn has_been_consumed(&self) -> bool {
        self.event_consumed
    }

    pub fn populate_osd(&mut self, osd: &mut Text) {
        // TODO have a way to toggle showing all actions!
        for a in &self.important_actions {
            osd.add_line(a.clone());
        }

        //println!("{}", self.unimportant_actions_tree);
    }

    fn reserve_key(&mut self, key: Key, action: &str) {
        if let Some(prev_action) = self.reserved_keys.get(&key) {
            println!("both {} and {} read key {:?}", prev_action, action, key);
        }
        self.reserved_keys.insert(key, action.to_string());
    }
}

pub(crate) struct ContextMenu {
    actions: BTreeMap<Key, String>,
    origin: Pt2D,
}

impl ContextMenu {
    pub(crate) fn draw(&self, g: &mut GfxCtx, canvas: &Canvas) {
        let mut txt = Text::new();
        for (hotkey, action) in &self.actions {
            txt.add_styled_line(describe_key(*hotkey), Color::BLUE, None);
            txt.append(format!(" - {}", action), TEXT_FG_COLOR, None);
        }
        canvas.draw_text_at(g, txt, self.origin);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}
