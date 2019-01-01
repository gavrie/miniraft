mod miniraft;

use self::miniraft::state::State;

fn main() {
    let state = State::new();
    println!("State: {:#?}", state);
}
