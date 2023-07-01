import bodyParser from "body-parser";
import createExpress from "express";
import type { Request, Response } from "express";

export const listen = async (port: number) => {
	const express = createExpress();
	express.use(bodyParser.json());

	await new Promise<void>((resolve) =>
		express.listen(port, () => {
			resolve();
		})
	);

	const route = <
		TBodySchema extends (z.ZodTypeAny | undefined) = undefined,
		TBody extends (
			TBodySchema extends undefined
			? unknown
			: z.infer<TBodySchema>
		) = unknown,

  		TPathSchema extends (z.ZodTypeAny | undefined) = undefined,
		TPath extends (
			TPathSchema extends undefined
			? unknown
			: z.infer<TPathSchema>
		) = unknown,

  		TQuerySchema extends (z.ZodTypeAny | undefined) = undefined,
		TQuery extends (
			TQuerySchema extends undefined
			? unknown
			: z.infer<TQuerySchema>
		) = unknown,

  		TResponseSchema extends (z.ZodTypeAny | undefined) = undefined,
	>(options: {
		method: 
			| "CONNECT"
			| "DELETE"
			| "GET"
			| "HEAD"
			| "OPTIONS"
			| "PATCH"
			| "POST"
			| "PUT"
			| "TRACE"
			| (string & {})
		,
		path: string,
		schemas?: undefined | {
			body?: TBodySchema,
			path?: TPathSchema,
			query?: TQuerySchema,
			response?: TResponseSchema,
		},
		handler: (req: Omit<Request, "body" | "params" | "query"> & {
			bodyParams: TBody,
			pathParams: TPath,
			queryParams: TQuery,
		}) => Promise<{
			body? undefined | Buffer | string,
			code: number,
			headers?: 
				| undefined
				| Record<string, undefined | string | Array<string>>
				| Array<[string, undefined | string | Array<string>]>
			,
		}>,
	}) => {
		const {
			method,
			path,
			schemas,
			handler,
		} = options;

		return express.all(path, async (req, res, next) => {
			try {
				if (req.method.toUpperCase() !== method.toUpperCase()) {
					return;
				}

				const bodyParams =
					schemas.body === undefined
					? req.body as unknown
					: schemas.body.parse(req.body)
				;
				const pathParams =
					schemas.path === undefined
					? req.params as unknown
					: schemas.path.parse(req.params)
				;
				const queryParams =
					schemas.query === undefined
					? req.query as unknown
					: schemas.query.parse(req.query)
				;
				const {
					body: _,
					params: _,
					query: _,
					...reqRest
				} = req;

				const response = await handler({
					...reqRest,
					bodyParams,
					pathParams,
					queryParams,
				});
				const validatedResponse =
					schemas.response === undefined
					? response
					: schemas.response.parse(response)
				;

				res.status(validatedResponse.code);

				if (
					true
					&& validatedResponse.headers !== undefined
					&& Array.isArray(validatedResponse.headers)
				) {
					for (const [key, value] of validatedResponse.headers) {
						res.append(key, value);
					};
				} else if (
					true
					&& validatedResponse.headers !== undefined
					&& typeof validatedResponse.headers === "object"
				) {
					for (const [key, value] of Object.entries(validatedResponse.headers)) {
						res.append(key, value);
					};
				};

				if (validatedResponse.body !== undefined) {
					res.send(validatedResponse.body);
				}
				
				res.end();
			} catch (error: unknown) {
				next(unknown)
			};
		}
	});

	return {
		express,
		route,
	};
};

