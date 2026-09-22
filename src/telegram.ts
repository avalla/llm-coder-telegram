import type {
  ChatGateway,
  ChatMessageOptions,
  IncomingMessage,
  MessageRef,
  TopicRef,
} from "./domain.js";

interface TelegramResponse<T> {
  ok: boolean;
  result?: T;
  description?: string;
}

interface TelegramMessage {
  message_id: number;
  message_thread_id?: number;
  chat: { id: number | string };
  from?: { id: number };
  text?: string;
}

interface TelegramUpdate {
  update_id: number;
  message?: TelegramMessage;
}

interface TelegramTopic {
  message_thread_id: number;
  name: string;
}

export class TelegramApiGateway implements ChatGateway {
  private readonly baseUrl: string;

  constructor(
    token: string,
    private readonly fetchImpl: typeof fetch = fetch,
  ) {
    if (!token) throw new Error("Telegram bot token is required");
    this.baseUrl = `https://api.telegram.org/bot${token}`;
  }

  async sendMessage(
    target: { chatId: string; threadId?: string },
    text: string,
    options?: ChatMessageOptions,
  ): Promise<MessageRef> {
    const result = await this.call<TelegramMessage>("sendMessage", {
      chat_id: target.chatId,
      text,
      ...(target.threadId ? { message_thread_id: Number(target.threadId) } : {}),
      ...(options?.buttons
        ? {
            reply_markup: {
              inline_keyboard: options.buttons.map((row) =>
                row.map((button) => ({ text: button.label, callback_data: button.callbackData })),
              ),
            },
          }
        : {}),
    });
    return target.threadId
      ? { chatId: target.chatId, threadId: target.threadId, messageId: String(result.message_id) }
      : { chatId: target.chatId, messageId: String(result.message_id) };
  }

  async editMessage(
    message: MessageRef,
    text: string,
    options?: ChatMessageOptions,
  ): Promise<void> {
    await this.call("editMessageText", {
      chat_id: message.chatId,
      message_id: Number(message.messageId),
      text,
      ...(options?.buttons
        ? {
            reply_markup: {
              inline_keyboard: options.buttons.map((row) =>
                row.map((button) => ({ text: button.label, callback_data: button.callbackData })),
              ),
            },
          }
        : {}),
    });
  }

  async sendDocument(
    target: { chatId: string; threadId?: string },
    filename: string,
    content: string,
  ): Promise<void> {
    const form = new FormData();
    form.set("chat_id", target.chatId);
    if (target.threadId) form.set("message_thread_id", target.threadId);
    form.set("document", new File([content], filename, { type: "text/plain" }));
    await this.callForm("sendDocument", form);
  }

  async createTopic(chatId: string, title: string): Promise<TopicRef> {
    const result = await this.call<TelegramTopic>("createForumTopic", {
      chat_id: chatId,
      name: title,
    });
    return { chatId, threadId: String(result.message_thread_id), title: result.name };
  }

  async closeTopic(topic: TopicRef): Promise<void> {
    await this.call("closeForumTopic", {
      chat_id: topic.chatId,
      message_thread_id: Number(topic.threadId),
    });
  }

  async poll(
    offset: number | undefined,
    signal?: AbortSignal,
  ): Promise<{ messages: readonly IncomingMessage[]; nextOffset?: number }> {
    const updates = await this.call<TelegramUpdate[]>(
      "getUpdates",
      {
        timeout: 30,
        allowed_updates: ["message"],
        ...(offset === undefined ? {} : { offset }),
      },
      signal,
    );
    const messages = updates.flatMap((update) => {
      const message = update.message;
      if (!message?.text || !message.from) return [];
      return [
        {
          chatId: String(message.chat.id),
          ...(message.message_thread_id === undefined
            ? {}
            : { threadId: String(message.message_thread_id) }),
          userId: String(message.from.id),
          text: message.text,
          messageId: String(message.message_id),
        },
      ];
    });
    const lastUpdateId = updates.at(-1)?.update_id;
    return lastUpdateId === undefined ? { messages } : { messages, nextOffset: lastUpdateId + 1 };
  }

  private async call<T>(
    method: string,
    body: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<T> {
    const response = await this.fetchImpl(`${this.baseUrl}/${method}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      ...(signal ? { signal } : {}),
    });
    const payload = (await response.json()) as TelegramResponse<T>;
    if (!response.ok || !payload.ok || payload.result === undefined) {
      throw new Error(`Telegram ${method} failed: ${payload.description ?? response.statusText}`);
    }
    return payload.result;
  }

  private async callForm<T>(method: string, body: FormData, signal?: AbortSignal): Promise<T> {
    const response = await this.fetchImpl(`${this.baseUrl}/${method}`, {
      method: "POST",
      body,
      ...(signal ? { signal } : {}),
    });
    const payload = (await response.json()) as TelegramResponse<T>;
    if (!response.ok || !payload.ok || payload.result === undefined) {
      throw new Error(`Telegram ${method} failed: ${payload.description ?? response.statusText}`);
    }
    return payload.result;
  }
}

export class SupervisedTaskSet {
  private readonly tasks = new Set<Promise<void>>();

  constructor(
    private readonly onError: (error: unknown) => void = (error) => console.error(error),
  ) {}

  spawn(task: Promise<void>): void {
    const observed = task.then(
      () => undefined,
      (error) => {
        this.onError(error);
      },
    );
    this.tasks.add(observed);
    void observed.then(() => this.tasks.delete(observed));
  }

  async drain(): Promise<void> {
    while (this.tasks.size > 0) {
      await Promise.all([...this.tasks]);
    }
  }
}

export async function runTelegramPolling(
  gateway: TelegramApiGateway,
  handler: (message: IncomingMessage) => Promise<void>,
  signal: AbortSignal,
  tasks = new SupervisedTaskSet(),
): Promise<void> {
  let offset: number | undefined;
  while (!signal.aborted) {
    let batch;
    try {
      batch = await gateway.poll(offset, signal);
    } catch (error) {
      if (!signal.aborted) throw error;
      break;
    }
    offset = batch.nextOffset ?? offset;
    for (const message of batch.messages) {
      tasks.spawn(Promise.resolve().then(() => handler(message)));
    }
  }
  await tasks.drain();
}
