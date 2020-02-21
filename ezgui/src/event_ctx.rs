use crate::{
    svg, text, Canvas, Color, Drawable, Event, GeomBatch, GfxCtx, Line, Prerender, ScreenPt, Text,
    UserInput,
};
use abstutil::{elapsed_seconds, Timer, TimerSink};
use geom::Polygon;
use instant::Instant;
use std::collections::VecDeque;

pub struct EventCtx<'a> {
    pub(crate) fake_mouseover: bool,
    pub input: UserInput,
    // TODO These two probably shouldn't be public
    pub canvas: &'a mut Canvas,
    pub prerender: &'a Prerender,
}

impl<'a> EventCtx<'a> {
    pub fn loading_screen<O, S: Into<String>, F: FnOnce(&mut EventCtx, &mut Timer) -> O>(
        &mut self,
        raw_timer_name: S,
        f: F,
    ) -> O {
        let timer_name = raw_timer_name.into();
        let mut timer = Timer::new_with_sink(
            &timer_name,
            Box::new(LoadingScreen::new(
                self.prerender,
                self.canvas.window_width,
                self.canvas.window_height,
                timer_name.clone(),
            )),
        );
        f(self, &mut timer)
    }

    pub fn canvas_movement(&mut self) {
        self.canvas.handle_event(&mut self.input)
    }

    // Use to immediately plumb through an (empty) event to something
    pub fn no_op_event<O, F: FnMut(&mut EventCtx) -> O>(
        &mut self,
        fake_mouseover: bool,
        mut cb: F,
    ) -> O {
        let mut tmp = EventCtx {
            fake_mouseover,
            input: UserInput::new(Event::NoOp, self.canvas),
            canvas: self.canvas,
            prerender: self.prerender,
        };
        cb(&mut tmp)
    }

    pub fn redo_mouseover(&self) -> bool {
        self.fake_mouseover
            || self.input.window_lost_cursor()
            || (!self.is_dragging() && self.input.get_moved_mouse().is_some())
            || self
                .input
                .get_mouse_scroll()
                .map(|(_, dy)| dy != 0.0)
                .unwrap_or(false)
    }

    pub fn normal_left_click(&mut self) -> bool {
        if self.input.has_been_consumed() {
            return false;
        }
        if !self.is_dragging() && self.input.left_mouse_button_released() {
            self.input.consume_event();
            return true;
        }
        false
    }

    fn is_dragging(&self) -> bool {
        self.canvas.drag_canvas_from.is_some() || self.canvas.drag_just_ended
    }

    // Delegation to assets
    pub fn default_line_height(&self) -> f64 {
        self.prerender.assets.default_line_height
    }

    // TODO I can't decide which way the API should go.
    pub fn upload(&self, batch: GeomBatch) -> Drawable {
        self.prerender.upload(batch)
    }
}

pub struct LoadingScreen<'a> {
    canvas: Canvas,
    prerender: &'a Prerender,
    lines: VecDeque<String>,
    max_capacity: usize,
    last_drawn: Instant,
    title: String,
}

impl<'a> LoadingScreen<'a> {
    pub fn new(
        prerender: &'a Prerender,
        initial_width: f64,
        initial_height: f64,
        title: String,
    ) -> LoadingScreen<'a> {
        let canvas = Canvas::new(initial_width, initial_height);
        let max_capacity = (0.8 * initial_height / prerender.assets.default_line_height) as usize;
        LoadingScreen {
            prerender,
            lines: VecDeque::new(),
            max_capacity,
            // If the loading callback takes less than 0.5s, we don't redraw at all.
            last_drawn: Instant::now(),
            title,
            canvas,
        }
    }

    fn redraw(&mut self) {
        // TODO Ideally we wouldn't have to do this, but text rendering is still slow. :)
        if elapsed_seconds(self.last_drawn) < 0.5 {
            return;
        }
        self.last_drawn = Instant::now();

        let mut txt = Text::from(Line(&self.title).roboto_bold());
        for l in &self.lines {
            txt.add(Line(l));
        }

        let mut g = GfxCtx::new(self.prerender, &self.canvas, false);
        g.clear(Color::BLACK);

        let mut batch = GeomBatch::from(vec![(
            text::BG_COLOR,
            Polygon::rectangle(0.8 * g.canvas.window_width, 0.8 * g.canvas.window_height),
        )]);
        batch.add_translated(
            txt.inner_render(&g.prerender.assets, svg::LOW_QUALITY),
            0.0,
            0.0,
        );
        let draw = g.upload(batch);
        g.redraw_at(
            ScreenPt::new(0.1 * g.canvas.window_width, 0.1 * g.canvas.window_height),
            &draw,
        );

        g.inner.finish();
    }
}

impl<'a> TimerSink for LoadingScreen<'a> {
    // TODO Do word wrap. Assume the window is fixed during loading, if it makes things easier.
    fn println(&mut self, line: String) {
        if self.lines.len() == self.max_capacity {
            self.lines.pop_front();
        }
        self.lines.push_back(line);
        self.redraw();
    }

    fn reprintln(&mut self, line: String) {
        self.lines.pop_back();
        self.lines.push_back(line);
        self.redraw();
    }
}
